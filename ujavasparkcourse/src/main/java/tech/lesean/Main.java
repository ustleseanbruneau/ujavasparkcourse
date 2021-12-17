package tech.lesean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		//  JavaRDD Setting up context
		//SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Java Spark - setting up a session
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				// if on Windows - need to add the following to SparkSession command				
				//.config("spark.sql.warehouse.dir","file:///c:/tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// show() will print out first 20 rows of dataset
		dataset.show();
		
		long numberOfRows = dataset.count();
		System.out.println("There are " + numberOfRows + " records.");
		
		spark.close();
				
		
		
		
	}

}
