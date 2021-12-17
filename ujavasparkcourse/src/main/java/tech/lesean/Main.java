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
		
		// show() will print out first 20 rows of dataset
		//dataset.show();
		
		//long numberOfRows = dataset.count();
		//System.out.println("There are " + numberOfRows + " records.");
		
		// Section 17
		//   single row
		//Row firstRow = dataset.first();
		
		// get(int index) - returns general object
		//String subject = firstRow.get(2).toString();
		//System.out.println(subject);
		
		// if using a header row, can use column name with .getAs(<<field_name>>) method.
		//String subject = firstRow.getAs("subject").toString();
		//System.out.println(subject);
		
		//int year = Integer.parseInt(firstRow.getAs("year"));
		//System.out.println("The year is: " + year);
		
		//Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");
		
		// Dataset with a Lambda function
		//Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art") 
		// && Integer.parseInt(row.getAs("year")) >= 2007);
		
//		Column subjectColumn = dataset.col("subject");
//		Column yearColumn = dataset.col("year");
		
		// Single column filter
		//Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art"));
		// Multiple columns filter
		//Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
		//															.and(yearColumn.geq(2007)));
		
		// using import static org.apache.spark.sql.functions.*;
		// could declare column this way, or directly in filter method
		//Column subjectColumn = col("subject");
		//Column yearColumn = col("year");
		
		//Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art")
		//															.and(yearColumn.geq(2007)));
		
		// using import static org.apache.spark.sql.functions.*;
		// Calling col(*) directly in filter method without Column declarations 
		Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
				.and(col("year").geq(2007)));
		
		modernArtResults.show();
		
		long numberOfRows = modernArtResults.count();
		System.out.println("There are " + numberOfRows + " records.");
		
		
		spark.close();
				
		
		
		
	}

}
