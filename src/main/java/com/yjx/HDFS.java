package com.yjx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Arrays;

/**
 * 上传数据集到HDFS
 */
public class HDFS {

    public static String FileName1 = "Q1.csv";
    public static String FileName2 = "Q2.csv";
    public static String FileName3 = "Q3.csv";
    public static String FileName4 = "Q4.csv";
    public static String FileName5 = "Q5.csv";

    public static void main(String[] args) throws IOException {
        //上传文件
        UpLoad();
    }

    private static void UpLoad() throws IOException {
        //加载配置文件
        Configuration configuration = new Configuration(true);

        //获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);

        //将文件上传至HDFS
        Path srcPath = new Path("D:\\桌面\\学习\\大数据\\实践\\Q\\" +FileName5);
        Path destPath = new Path("/yjx/");

        fileSystem.copyFromLocalFile(srcPath, destPath);

        //关闭连接
        fileSystem.close();
    }
}