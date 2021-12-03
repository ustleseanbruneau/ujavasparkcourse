package tech.lesean;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
		
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		
		Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
		
		JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );
		
		//sqrtRdd.foreach( System.out::println );
		sqrtRdd.collect().forEach( value -> System.out.println(value));
		
		System.out.println(result);
		
		// how many elements in sqrtRdd
		// using just map and reduce
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
		Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println(count);
		
		sc.close();

	}

}
