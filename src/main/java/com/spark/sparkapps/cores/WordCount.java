package com.spark.sparkapps.cores;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//第一步，创建sparkconf对象
		SparkConf conf = new SparkConf().setAppName("MyWordCountApp").setMaster("local");
		System.out.println("1.完成第一步........");
		//第二步，创建sparkcontext对象
		//sparkcontext是spark程序的入口
		JavaSparkContext sc = new JavaSparkContext(conf);
		System.out.println("2.完成第二部........");
		//第三步，读取文件
		JavaRDD<String> lines = sc.textFile("hdfs://192.168.200.97:9000/WordCount/EnglishArticle.txt");
		//JavaRDD<String> lines = sc.textFile("C://EnglishArticle.txt");
		System.out.println("3.完成第三步........");
		//第四步，对RDD进行操作
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split(" "));
			}
		});//对每一行安空格对单词进行切分
		
		JavaPairRDD<String, Integer> wordpair = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(word, 1);
			}
		});//在拆分单词的基础上对每个单词计数为1
		
		JavaPairRDD<String, Integer> wordcount = wordpair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer num1, Integer num2) throws Exception {
				// TODO Auto-generated method stub
				return num1 + num2;
			}
		});//累计所有单词数
		
		//打印
		wordcount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			public void call(Tuple2<String, Integer> pairs) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(pairs._1+" : "+pairs._2);
			}
		});
		wordcount.saveAsTextFile("hdfs://192.168.200.97:9000/WordCount/res.txt");
		sc.close();

	}

}
