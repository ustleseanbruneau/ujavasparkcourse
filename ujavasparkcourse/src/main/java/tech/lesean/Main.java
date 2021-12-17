package tech.lesean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Main {

	public static void main(String[] args) {
		
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		// Java Spark - setting up a session
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.getOrCreate();
		
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// Section 18 - The Full SQL Syntax
		// SQL statement does not work in filter method
		//Dataset<Row> modernArtResults = dataset.filter("select * from students where subject = 'Modern Art' AND year >= 2007 ");
		//modernArtResults.show();
		
		// temporary view - in memory data structure
		dataset.createOrReplaceTempView("my_students_table");
		
		//Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'French' AND year >= 2007 ");
		//Dataset<Row> results = spark.sql("select score,year from my_students_table where subject = 'French' AND year >= 2007 ");
		//Dataset<Row> results = spark.sql("select min(score) from my_students_table where subject = 'French' AND year >= 2007 ");
		//Dataset<Row> results = spark.sql("select avg(score) from my_students_table where subject = 'French' AND year >= 2007 ");
		//Dataset<Row> results = spark.sql("select max(score) from my_students_table where subject = 'French' AND year >= 2007 ");
		
		//Dataset<Row> results = spark.sql("select distinct(year) from my_students_table ");
		//Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year");
		Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
		
		results.show();
		
		spark.close();
				
		
		
		
	}

}
