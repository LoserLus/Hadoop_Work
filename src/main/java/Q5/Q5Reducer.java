package Q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Q5Reducer extends Reducer<Text, IntWritable, Q5, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Q5, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取迭代器
        Iterator<IntWritable> iterator = values.iterator();

        //声明一个计数器
        int min = 999;
        int temp = 0;
        //求时间差最小值
        while (iterator.hasNext()) {
            temp = iterator.next().get();
            if(min>temp)
                min = temp;
        }

        System.out.println(key.toString());
        //对year:name:artist进行拆分
        String[] pf = key.toString().split(":");

        //创建对象
        Q5 q;
        //合并歌手
        if(pf.length>3){
            //有多名歌手，通过斜杠连接
            StringBuffer sb = new StringBuffer();
            sb.append(pf[2]);
            for(int i=3;i< pf.length;i++)
                sb.append("/").append(pf[i]);

            q = new Q5(0,Integer.parseInt(pf[0]),pf[1],sb.toString(),min);

        }else{
            //只有一名歌手
            //存入Q1对象
            q = new Q5(0,Integer.parseInt(pf[0]),pf[1],pf[2],min);
        }

        context.write(q,NullWritable.get());
    }
}
