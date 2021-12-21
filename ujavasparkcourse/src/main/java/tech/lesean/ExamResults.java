package tech.lesean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class ExamResults {

	public static void main(String[] args) {
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		// Java Spark - setting up a session
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
				.getOrCreate();
		

		// Section 26 - More Aggregations		

		//Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// doesn't work with default data load - column "score" not a number
		//dataset = dataset.groupBy("subject").max("score");
		
		// change option to read in csv file
		// use inferSchema to automatically load with data
		//Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/exams/students.csv");
		
		
		// does work with option inferSchema data load 
		//dataset = dataset.groupBy("subject").max("score");
		
		// Example - using a Column object to convert data
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		//Column score = dataset.col("score");
		//dataset = dataset.groupBy("subject").max(score.cast(DataTypes.IntegerType));
		
		// Note using max function from 
		//  import static org.apache.spark.sql.functions.*;
		//dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType) ).alias("max score") );
		
		// Example with two agg columns
		//dataset = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType) ).alias("max score"),
		//										min(col("score").cast(DataTypes.IntegerType) ).alias("min score"));
		
		// Example with agg function - don't need to cast datatypes
		dataset = dataset.groupBy("subject").agg(max(col("score") ).alias("max score"),
												min(col("score") ).alias("min score"));
		
		
		// Section 27 - Practical Exercise
		
		//dataset = dataset.groupBy("subject").pivot("year").agg(avg(col("score")));
		
		// rounded to two decimal places
		//dataset = dataset.groupBy("subject").pivot("year").agg( round( avg(col("score")), 2 ));
		
		// Adding standard deviation
		//dataset = dataset.groupBy("subject").pivot("year").agg( round( avg(col("score")), 2 ), 
		//														round( stddev(col("score")), 2 ) );
		
		// Adding alias for column header
		//dataset = dataset.groupBy("subject").pivot("year").agg( round( avg(col("score")), 2 ).alias("average"), 
		//														round( stddev(col("score")), 2 ).alias("stddev") );
		
		dataset.show();
		
	}

}
