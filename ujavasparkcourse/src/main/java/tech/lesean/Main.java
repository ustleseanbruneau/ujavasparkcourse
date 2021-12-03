package tech.lesean;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
		
		// First line - traditional Java with separate class
		//JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map( value -> new IntegerWithSquareRoot(value) );
		// Second line - new Scale Tuple2 type - don't need separate Java class
		JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map( value -> new Tuple2<>(value, Math.sqrt(value)) );
		
		// Examples of Tuples with different sizes
		// new Tuple5(1, 4, 7, 3, 2)
		// max size of Tuple = Tuple22
		
		sc.close();

	}

}
