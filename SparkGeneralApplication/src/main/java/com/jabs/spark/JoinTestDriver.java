package com.jabs.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;


public class JoinTestDriver {

	public static void main(String[] args) {
		System.out.println("Arguments: " + Arrays.toString(args));
		
		SparkConf conf = new SparkConf().setAppName("PersistedJoinTestDriver");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> firstRdd = jsc.textFile(args[0]);
		JavaRDD<String> secondRDD = jsc.textFile(args[1]);
		
		LongAccumulator firstRDDRecordCount = jsc.sc().longAccumulator("FirstRDD:recordCount");
		LongAccumulator secondRDDRecordCount = jsc.sc().longAccumulator("SecondRDD:recordCount");
		
		JavaPairRDD<String, Integer> firstPairRdd = firstRdd.mapToPair(line -> {String[] splits = line.split(","); firstRDDRecordCount.add(1);
																				return new Tuple2<String, Integer>(splits[0], Integer.parseInt(splits[1]));
																		});
		firstPairRdd.persist(StorageLevel.MEMORY_ONLY());
		
		JavaPairRDD<String, Integer> secondPairRdd = secondRDD.mapToPair(line -> {String[] splits = line.split(","); secondRDDRecordCount.add(1);
																				return new Tuple2<String, Integer>(splits[0], Integer.parseInt(splits[1]));
																		});
		secondPairRdd.persist(StorageLevel.MEMORY_ONLY());
		
		System.out.println("firstRDDRecordCount: " + firstRDDRecordCount.value());
		System.out.println("secondRDDRecordCount: " + secondRDDRecordCount.value());
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = firstPairRdd.join(secondPairRdd);
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftJoinedRDD = firstPairRdd.leftOuterJoin(secondPairRdd);
		JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightJoinedRDD = firstPairRdd.rightOuterJoin(secondPairRdd);
		JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> fullJoinedRDD = firstPairRdd.fullOuterJoin(secondPairRdd);
		
		System.out.println("joinedRDD is " + joinedRDD.collect());
		System.out.println("leftJoinedRDD is " + leftJoinedRDD.collect());
		System.out.println("rightJoinedRDD is " + rightJoinedRDD.collect());
		System.out.println("fullJoinedRDD is " + fullJoinedRDD.collect());
		
		jsc.close();
	}

}
