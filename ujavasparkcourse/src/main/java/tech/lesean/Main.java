package tech.lesean;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
		

		// Section 24 - DataFrames API		
		// using bigfile
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		// just a single column result
		//dataset = dataset.select("level");
		
		// error because not a column name
		//dataset = dataset.select("level", "date_format(datetime,'MMMM')");
		
		// Example with selectExpr to get datetime of Month
		//dataset = dataset.selectExpr("level","date_format(datetime,'MMMM') as month");
		
		// Example with spark function - date_format with column and date format as parameter
		// need to be consistent - both columns or both string names
		// .alias("") - to add a column alias
		//dataset = dataset.select(col("level"),date_format(col("datetime"),"MMMM").alias("month"));
		
		// Grouping - add count() for aggregation
		//dataset = dataset.groupBy(col("level"), col("month")).count();
		
		// Order by month
		//  Need to add a monthnum column
		dataset = dataset.select(col("level"),
				date_format(col("datetime"),"MMMM").alias("month"), 
				date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		dataset = dataset.groupBy(col("level"), col("month"),col("monthnum")).count();
		dataset = dataset.orderBy(col("monthnum"), col("level"));
		dataset = dataset.drop(col("monthnum"));
		
		
		dataset.show(100);
		
		spark.close();		
		
	}

}
