package com.jabs.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountDriver {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("WordCountDriver");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> textFileRDD = jsc.textFile(args[0]);
		//Create PairRDD of words and its count as 1
		JavaPairRDD<String, Integer> pairRDD = textFileRDD.flatMapToPair(t -> {List<Tuple2<String, Integer>> list = new ArrayList<>();
																				for(String split : t.split(" ")) 
																					list.add(new Tuple2<String, Integer>(split, 1));
																				return list.iterator();});
				
		JavaPairRDD<String, Integer> reducedRDD = pairRDD.reduceByKey((v1,v2) -> v1+v2);
		
		reducedRDD.saveAsTextFile(args[1]);
		
		jsc.close();
	}

}
