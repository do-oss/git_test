package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple2$;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class SparkWordCount {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> fileRDD = sc.textFile("src/mian/files/words.txt");
        
        //flatMap()算子用来进行分割操作 将原RDD中的数据分成一个个片段
        // new FlatMapFunction<String, String>中的两个String分别表示输入和输出类型
        JavaRDD<String>wordRDD =  fileRDD.flatMap(new FlatMapFunction<String, String>(){
            @Override
            public Iterator<String> call(String line) throws Exception{
                //通过迭代器将分割后的数据元素全部返回输出
                return Arrays.asList(line.split("\\s+")).iterator();
            }
        });
        
        //mapToPair()算子是用来对分割后的一个个片段结果添加计数标志的，如出现次数1，该函数用来创建并返回pair类型的RDD new PairFunction
        JavaPairRDD<String, Integer> wordOneRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer>call(String word) throws Exception{
                return new Tuple2<>(word, 2);//Tuple2是spark的二元数组类型，java中没有
            }
        });

        //reduceByKey()算子是根据key来聚合，reduce阶段.new Function2<Integer,Integer,Integer>中分别是用来聚合的两个输入类型Integer,Integer和聚合后的输出类型Integer
        JavaPairRDD<String, Integer>wordCountRDD = wordOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        wordCountRDD.saveAsTextFile("E:\\result7");
    }
}

