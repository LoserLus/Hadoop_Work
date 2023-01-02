package Q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Q2Reducer extends Reducer<Text, IntWritable, Q2, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Q2, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取迭代器
        Iterator<IntWritable> iterator = values.iterator();

        //声明一个计数器
        int sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }

        //对year:歌手1:歌手2..进行拆分
        String[] pf = key.toString().split(":");

        //用斜杠合并歌手
        StringBuffer sb = new StringBuffer();

        //先载入一名歌手
        sb.append(pf[1]);

        //若有多名歌手
        if(pf.length>3){
            for(int i=2;i< pf.length;i++){
                sb.append("/").append(pf[i]);
            }
        }


        //存入Q1对象
        Q2 q = new Q2(0,Integer.parseInt(pf[0]),sum,sb.toString());

        context.write(q,NullWritable.get());
    }
}
