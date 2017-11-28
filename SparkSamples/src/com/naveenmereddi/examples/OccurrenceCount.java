package com.naveenmereddi.examples;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class OccurrenceCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("OccurrenceCount").setMaster("local[1]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("input/Padmavati.txt");
		
		JavaRDD<String> occurrences = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		
		Map<String, Long> count = occurrences.countByValue();
		
		count.forEach((k,v) -> System.out.println(k + ":" + v));
		
		sc.close();
	}

}
