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
		

		// Section 23 - Ordering
		// Any column which is not part of the "grouping" must have an 
		//	Aggregation function performed on it
		
		// using bigfile
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		dataset.createOrReplaceTempView("logging_table");
		
		
		// add monthnum column to dataset in query
		//   used for ordering
//		Dataset<Row> results = spark.sql
//				("select level, date_format(datetime,'MMMM') as month, "
//						+ "cast(first(date_format(datetime,'M')) as int ) as monthnum, count(1) as total "
//						+ "from logging_table group by level, month order by monthnum");
//		
//		// remove monthnum column from dataset
//		results.drop("monthnum");
		
		// optimization - without temporary column monthnum
		Dataset<Row> results = spark.sql
		("select level, date_format(datetime,'MMMM') as month, count(1) as total "
				+ "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int ), level ");

		results.show(100);
		
		spark.close();		
		
	}

}
