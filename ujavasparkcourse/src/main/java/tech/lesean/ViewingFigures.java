package tech.lesean;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");

		//Logger.getLogger("org.apache").setLevel(Level.WARN);
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// Warmup
		JavaPairRDD<Integer, Integer> chapterCountRdd = chapterData.mapToPair( row -> new Tuple2<Integer, Integer>(row._2, 1))
		           .reduceByKey( (value1, value2) -> value1 + value2);
		// test - creating chapterCountRdd
		//chapterCountRdd.foreach( value -> System.out.println(value) );
		
		// Step 1 - remove any duplicated views
		//   end up with viewData RDD (userId, chapterId)
		viewData = viewData.distinct();
		// test - remove dup views in viewData RDD
		//viewData.foreach( value -> System.out.println(value) );
		
		// Step 2 - get the course Ids into the RDD
		// flip columns in viewData RDD - end up with (chapterId, userId)
		viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1 ));
		//viewData.foreach( value -> System.out.println(value) );
		//  Join view and chapter date - Key will be Integer, value will be Tulpe2<Integer, Integer>
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = viewData.join(chapterData);
		//joinedRdd.foreach( value -> System.out.println(value) );
		
		// Step 3 - don't need chapterIds, setting up for a reduce
		//  toughest step - need to deal with Tuple2 objects
		//  key - Tuple2<Integer, Integer> and value - Long
		//  { } -> allow for multiple lines of code
		JavaPairRDD<Tuple2<Integer, Integer>, Long > step3 = joinedRdd.mapToPair( row -> {
			Integer userId = row._2._1;
			Integer courseId = row._2._2;
			return new Tuple2<Tuple2<Integer, Integer>, Long>( new Tuple2<Integer, Integer>(userId, courseId), 1L);
		});
		//step3.foreach( value -> System.out.println(value) );
		
		
		// Step 4 - count how many views for each user per course
		//  For user, chapter viewed twice result is -> ((14,1),2)
		step3 = step3.reduceByKey((value1, value2) -> value1+value2);
		//step3.foreach( value -> System.out.println(value) );
		
		// step 5 - remove the userIds
		//  step will simplify data by removing userId
		//  userId isn't needed going further
		JavaPairRDD<Integer, Long> step5 = step3.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));
		//step5.foreach( value -> System.out.println(value) );
		
		// step 6 - add in the total chapter count
		//  inner join keyed on courseId
		//  key - joins value - chapterId and views of chapters
		JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCountRdd);
		//step6.foreach( value -> System.out.println(value) );
		
		// step 7 - convert to percentage
		//  mapValue - not changing key, only changing values in RDD
		JavaPairRDD<Integer, Double> step7 = step6.mapValues(value -> (double)value._1 / value._2);
		//step7.foreach( value -> System.out.println(value) );
		
		// step 8 - convert to scores
		JavaPairRDD<Integer, Long> step8 = step7.mapValues(value -> {
			if (value > 0.9) return 10L;
			if (value > 0.5) return 4L;
			if (value > 0.25) return 2L;
			return 0L;
		});
		//step8.foreach( value -> System.out.println(value) );
		
		// step 9
		step8 = step8.reduceByKey((value1, value2) -> value1+value2);
		//step8.foreach( value -> System.out.println(value) );
		
		// step 10
		//  Join with course title and viewing results
		JavaPairRDD<Integer, Tuple2<Long, String>> step10 = step8.join(titlesData);
		
		//  Remove chapter id, just have 
		//  key - scoreResult value - courseTitle
		JavaPairRDD<Long, String> step11 = step10.mapToPair(row -> new Tuple2<Long, String>(row._2._1, row._2._2));
		//step11.sortByKey(false).collect().forEach(System.out::println);		
		step11.sortByKey(false).collect().forEach( value -> System.out.println(value) );		
		
		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
