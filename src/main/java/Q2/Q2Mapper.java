package Q2;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();

        //跳过表头
        if(line.contains("Year"))
            return;

        //去掉换行符
        line = line.replaceAll("\n","");

        //按逗号分割
        String[] pf = line.split(",");

        //合并年份和歌手，格式：year:歌手1:歌手2...
        StringBuffer sb = new StringBuffer();
        sb.append(pf[1]);

        for(int i=2;i< pf.length;i++){
            sb.append(":").append(pf[i]);
        }

        //输出数据
        //防止脏数据，判断是否能转化为整型
        if(!NumberUtils.isNumber(pf[0]))
            return;

        Integer time = Integer.parseInt(pf[0]);


        context.write(new Text(sb.toString()),new IntWritable(time));

    }
}
