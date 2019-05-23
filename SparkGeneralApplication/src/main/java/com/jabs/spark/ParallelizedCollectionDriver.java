package com.jabs.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ParallelizedCollectionDriver {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JabsParallelizedCollection");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//Parallelize a collection
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> parallelizedRDD = jsc.parallelize(data);
		int reducedValue = parallelizedRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 683723099423456134L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		System.out.println("Reduced value is " + reducedValue);
		
		jsc.close();
	}

}
