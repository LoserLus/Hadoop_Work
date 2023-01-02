package Q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Q1Reducer extends Reducer<Text, IntWritable, Q1, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Q1, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取迭代器
        Iterator<IntWritable> iterator = values.iterator();

        //声明一个计数器
        int sum = 0;
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }

        //对2019:Pop进行拆分
        String[] pf = key.toString().split(":");

        //存入Q1对象
        Q1 q = new Q1(0,Integer.parseInt(pf[0]),pf[1],sum);

        context.write(q,NullWritable.get());
    }
}
