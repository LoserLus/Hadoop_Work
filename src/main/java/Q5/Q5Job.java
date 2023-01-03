package Q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 5.统计每一年的歌曲首发与上榜时间差最小值
 */
public class Q5Job {
    private static String driverClass = "com.mysql.cj.jdbc.Driver";
    private static String url = "jdbc:mysql://192.168.88.100:3306/yjxxt?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false";
    private static String username = "root";
    private static String password = "Yjx#1234";

    //表信息
    private static String tableName = "Q5";
    private static String[] fields = {"id", "year", "name", "artist","maxdelta"};


    public static void main(String[] args) throws Exception {
        //加载配置文件
        Configuration configuration = new Configuration(true);
        configuration.set("mapreduce.framework.name", "local");

        //开始载入数据库的配置文件
        DBConfiguration.configureDB(configuration, driverClass, url, username, password);

        //创建JOB
        Job job = Job.getInstance(configuration);

        //设置Job
        job.setJobName("yjxxt-MusicCharts-mysql-" + new SimpleDateFormat("yyyyMMdd-HHmmss-SSS").format(new Date()));
        job.setJarByClass(Q5Job.class);
        job.setNumReduceTasks(2);

        //设置读取和写出的HDFS地址
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/yjx/Q5.csv"));
        DBOutputFormat.setOutput(job, tableName, fields);

        //设置Map和Reduce类以及传输的数据类型
        job.setMapperClass(Q5Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Q5Reducer.class);

        //提交任务
        job.waitForCompletion(true);

    }
}
