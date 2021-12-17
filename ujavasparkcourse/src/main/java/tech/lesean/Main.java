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
		

		// Section 20 - Grouping and Aggregations
		List<Row> inMemory = new ArrayList<Row>();

		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

		StructField[] fields = new StructField[] {
			new StructField("level", DataTypes.StringType, false, Metadata.empty()),
			new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};

		StructType schema = new StructType(fields);
		Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
		
		dataset.createOrReplaceTempView("logging_table");
		
		//Dataset<Row> results = spark.sql("select level, count(datetime) from logging_table group by level order by level");
		Dataset<Row> results = spark.sql("select level, collect_list(datetime) from logging_table group by level order by level");

		results.show();

		spark.close();
				
		
		
		
	}

}
