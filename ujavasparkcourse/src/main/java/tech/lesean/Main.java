package tech.lesean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		JavaRDD<String> sentences = sc.parallelize(inputData);
//		
//		JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//		
//		JavaRDD filteredWords = words.filter(word -> word.length() > 1);
//		
//		filteredWords.foreach(value -> System.out.println(value));
		
		// same as previous four lines - using consolodated coding
		sc.parallelize(inputData)
			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
			.filter(word -> word.length() > 1)
			.foreach(value -> System.out.println(value));
		
		sc.close();

	}

}
