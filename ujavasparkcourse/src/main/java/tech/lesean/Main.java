package tech.lesean;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		// Windows machines
		//System.setProperty("hadoop.home.dir", "c:/opt/hadoop");
		
 		// Get rid of extra logging
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName("First Spark Java Program").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4,18));
		visitsRaw.add(new Tuple2<>(6,4));
		visitsRaw.add(new Tuple2<>(10,9));
		
		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1, "John"));
		usersRaw.add(new Tuple2<>(2, "Bob"));
		usersRaw.add(new Tuple2<>(3, "Alan"));
		usersRaw.add(new Tuple2<>(4, "Doris"));
		usersRaw.add(new Tuple2<>(5, "Marybelle"));
		usersRaw.add(new Tuple2<>(6, "Raquel"));
		
		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
		
		// Left outer join
		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
		// .orElse() - default a value if not exists
		joinedRdd.foreach( value -> System.out.println(value) );
		//joinedRdd.foreach( it -> System.out.println(it._2._2.orElse("blank").toUpperCase() ));

		// right outer join
		//JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
		//joinedRdd.foreach( it -> System.out.println(" user " + it._2._2 + " had " + it._2._1.orElse(0) ));

		// cartesian
		//JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRdd = visits.cartesian(users);
		//joinedRdd.foreach( value -> System.out.println(value) );

		
		sc.close();

	}

}
