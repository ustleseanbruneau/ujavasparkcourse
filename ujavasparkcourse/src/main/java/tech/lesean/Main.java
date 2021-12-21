package tech.lesean;

import java.text.SimpleDateFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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
		

		// Section 28 - User Defined Functions		
		// using bigfile
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		
		spark.udf().register("monthNum", (String month) -> {
			java.util.Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		}, DataTypes.IntegerType );
		
		dataset.createOrReplaceTempView("logging_table");
		
		//Dataset<Row> results = spark.sql
		//		("select level, date_format(datetime,'MMMM') as month, count(1) as total "
		//				+ "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int ), level ");

		// new using UDF
		Dataset<Row> results = spark.sql
				("select level, date_format(datetime,'MMMM') as month, count(1) as total "
						+ "from logging_table group by level, month order by monthNum(month), level ");

		results.show(100);
		
		spark.close();		
		
	}

}
