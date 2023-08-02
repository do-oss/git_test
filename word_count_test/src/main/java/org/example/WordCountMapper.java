package org.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    /*
    参数：
    key:k1行偏移量
    value：v1每一行的文本数据
    context:表示上下文对象
     */
    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        //进行文本数据切分
        String[] split = value.toString().split(",");

        //1.遍历数组，组装k2和v2
        for(String word:split){
            //3.将k2和v2写入上下文中
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }

    }
}
