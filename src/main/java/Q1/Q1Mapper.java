package Q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();

        //跳过表头
        if(line.contains("year"))
            return;

        //去掉换行符
        line = line.replaceAll("\n","");

        //按逗号分割
        String[] pf = line.split(",");

        //获取年份
        String year = pf[0];
        for(int i=1;i<pf.length;i++){

            //拆分genre，格式：2019:Pop
            String newLine = year+":"+pf[i];
            //输出数据
            context.write(new Text(newLine),new IntWritable(1));
        }

    }
}
