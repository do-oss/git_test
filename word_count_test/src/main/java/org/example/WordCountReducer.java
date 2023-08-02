package org.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    int sum;
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key,Iterable<LongWritable>values, Context context) throws IOException, InterruptedException{
        sum = 0;
        //1 累加求和
        for (LongWritable count:values){
            sum += count.get();
        }
        //2 输出
        v.set(sum);
        context.write(key,v);
    }
}
