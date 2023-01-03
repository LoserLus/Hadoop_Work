package Q5;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
        if(pf.length<4)
            return;
        if(pf[0].length()<4||pf[1].length()<4||pf[2].length()<4||pf[3].length()<4||pf[4].length()<4)
            return;

        //获取年份
        String year = pf[0];


        //计算时间差
        //防止脏数据，判断是否能转化为整型
//        if(!NumberUtils.isNumber(pf[1]))
//            return;
//        Integer time = Integer.parseInt(pf[1]);
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        String closeDate = StringUtils.deleteWhitespace(pf[1]);
        String startDate = StringUtils.deleteWhitespace(pf[2]);
//        System.out.println(closeDate+"......"+startDate);
        int days = -1;
        try {
            days = (int)((df.parse(closeDate).getTime()-df.parse(startDate).getTime())/(24 * 60 * 60 * 1000));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(days <0)return;

        //System.out.println(days);
        //获取歌名
        String name = pf[3];

        //整合数据，格式：year:name:artist
        StringBuffer sb = new StringBuffer();
        sb.append(year).append(":").append(name);
        for(int i=4;i< pf.length;i++)
            sb.append(":").append(pf[i]);


        if(sb.toString().contains("\""))
            return;
//        System.out.println(sb.toString());
        context.write(new Text(sb.toString()),new IntWritable(days));
    }
}
