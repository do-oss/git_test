package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

public class testHDFS {
    public static void main(String[] args) throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://IP地址:9000"),new Configuration(),"root");
        //创建目录
        fs.mkdirs(new Path("/hello/nihao/wohenhao"));
        //copyFromLocalFile方法第一个参数为待上传文件的命令

    }
}
