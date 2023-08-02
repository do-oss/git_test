package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args)throws Exception {
        System.out.println("Hello world!");
        Configuration conf = new Configuration();
        //启动job任务
        int run = ToolRunner.run(conf, new JobMain(),args);
        System.exit(run);
    }
}