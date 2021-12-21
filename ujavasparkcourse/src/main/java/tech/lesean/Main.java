package tech.lesean;

import java.util.ArrayList;
import java.util.Arrays;
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
		

		// Section 25 - Pivot Tables		
		// using bigfile
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		// Order by month
		//  Need to add a monthnum column
		dataset = dataset.select(col("level"),
				date_format(col("datetime"),"MMMM").alias("month"), 
				date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
		
		// simple pivot table with row (level) and column (month)
		//dataset = dataset.groupBy("level").pivot("month").count();
		
		// pivot table with month number - ordered correctly, but doesn't have month name as column header
		//dataset = dataset.groupBy("level").pivot("monthnum").count();
		
		// simple pivot table with showing one month (column)
		//List<Object> columns = new ArrayList<Object>();
		//columns.add("March");
		
		//dataset = dataset.groupBy("level").pivot("month", columns).count();
		
		// Using object list with month names
		//  Object is ordered, so columns will appear in correct monthly order
		//  Good if you know column names in advance
		Object[] months = new Object[] { 
				"January", "February", "March", "April", "May", "June",
				"July", "August", "September", "October", "November", "December"
		};
		//List<Object> columns = Arrays.asList(months);
		
		//dataset = dataset.groupBy("level").pivot("month", columns).count();
		
		
		// Example to handle no values
		//  Adding month "Augtober"
		//Object[] months = new Object[] { 
		//		"January", "February", "March", "April", "May", "June",
		//		"July", "August", "September", "Augtober", "October", "November", "December"
		//};
		List<Object> columns = Arrays.asList(months);
		
		// add .na() and .fill(0) to populate a zero in case of missing value
		dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);
		
		dataset.show(100);
		
		spark.close();		
		
	}

}
