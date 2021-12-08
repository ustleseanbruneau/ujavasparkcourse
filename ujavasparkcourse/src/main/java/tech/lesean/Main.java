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
		
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		// same as previous four lines - using consolodated coding
		initialRdd
			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
			.foreach(value -> System.out.println(value));
		
		sc.close();

	}

}
