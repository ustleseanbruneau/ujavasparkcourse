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
		

		// Section 28 - User Defined Functions	
		
		//spark.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType );

		// previous statement with { }
		//spark.udf().register("hasPassed", (String grade) -> { 
		//	return grade.equals("A+");
		//}, DataTypes.BooleanType );

		// Grade A, B, or C is Pass
		//spark.udf().register("hasPassed", (String grade) -> { 
		//	return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		//}, DataTypes.BooleanType );

		// Two conditions
		spark.udf().register("hasPassed", (String grade, String subject) -> { 
			
			if (subject.equals("Biology")) {
				if (grade.startsWith("A")) return true;
				return false;
			}
			return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
		}, DataTypes.BooleanType );

		// Example - using a Column object to convert data
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		// lit - passing in literal value
		//dataset = dataset.withColumn("pass", lit( "YES"));
		
		// New column based on condition (true or false)
		//dataset = dataset.withColumn("pass", lit( col("grade").equalTo("A+")));
		
		// New column using UDF
		//dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade")) );
		
		// Using UDF with two column parameters
		dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")) );
		
		dataset.show();
		
	}

}
