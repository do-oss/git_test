package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 *
 */

public class Sparkdemo {
    public static void main(String[] args){
        String readme = "D:\\spark\\CHANGES.txt";
        SparkConf conf = new SparkConf().setAppName("tiger's first spark app ");
        conf.setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //从指定的文件中读取数据到RDD
        JavaRDD<String> logData = sc.textFile(readme).cache();

        //过滤包含h的字符串，然后在获取数量
        long num = logData.filter(new Function<String,Boolean>(){
            public Boolean call(String s){
                return s.contains("h");
            }
        }).count();

        System.out.println("the count of word a is " + num);
    }

}
/*
conf.setMaster()的参数及含义如下：
local本地单线程
local[k]本地多线程（指定k个内核）
local[*]本地多线程
spark://HOST:PORT 连接到指定的集群 需要指定端口
yarn-client客户端模式 连接到YARN集群 需要配置HADOOP_CONF_DIR
yarn-cluster集群模式 连接到YARN集群 需要配置HADOOP_CONF_DIR
 */