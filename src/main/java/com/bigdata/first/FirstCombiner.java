package com.bigdata.first;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dario
 */
public class FirstCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        while(values.iterator().hasNext()){
            sum += values.iterator().next().get();
        }
        context.write(key, new IntWritable(sum));
    }
}
