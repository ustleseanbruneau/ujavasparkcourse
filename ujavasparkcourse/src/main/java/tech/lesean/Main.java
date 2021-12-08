package tech.lesean;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		//JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input-spring.txt");
		
		// RegEx - [^a-zA-Z\\s] - if not a letter or space, replace
		// all lowercase letters - for grouping and sorting
		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );
		
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0);

		JavaRDD<String> justWords = removedBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
		
		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
		
		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
		
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));
		
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
		//List<String> results = justInterestingWords.take(50);
		List<Tuple2<Long, String>> results = sorted.take(10);
		
		results.forEach(System.out::println);
		
		sc.close();

	}

}
