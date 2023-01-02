package Q5;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();

        //跳过表头
        if(line.contains("Year"))
            return;

        //去掉换行符
        line = line.replaceAll("\n","");

        //按逗号分割 例：2019:92:Old Town Road:Lil Nas
        String[] pf = line.split(",");
        //防止脏数据
        if(pf.length<4)return;

        //获取年份
        String year = pf[0];

        //获取时间差
        //防止脏数据，判断是否能转化为整型
        if(!NumberUtils.isNumber(pf[1]))
            return;
        Integer time = Integer.parseInt(pf[1]);

        //获取歌名
        String name = pf[2];

        //整合数据，格式：year:name:artist
        StringBuffer sb = new StringBuffer();
        sb.append(year).append(":").append(name);
        for(int i=3;i< pf.length;i++)
            sb.append(":").append(pf[i]);

        context.write(new Text(sb.toString()),new IntWritable(time));
    }
}
