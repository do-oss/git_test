package org.example;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class JobMain extends Configured implements Tool {

    /*该方法用于指定一个job任务*/
    @Override
    public int run(String[] strings) throws Exception {
        //1、创建一个job
        Job job = Job.getInstance(super.getConf(),"WordCount");
        //2、配置job任务对象（八个步骤）
        //第一步 指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://brace:8020/wordcount"));
        //第二步 指定map阶段的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三、四、五、六步：shuffle 采用默认的方式

        //第七步：指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //第八步 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出路径
        TextOutputFormat.setOutputPath(job,new Path("hdfs://brace:8020/wordcount_out"));

        //等待任务结束
        boolean b1 = job.waitForCompletion(true);
        return b1?0:1;
    }
}
