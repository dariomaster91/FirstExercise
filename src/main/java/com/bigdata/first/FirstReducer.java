package com.bigdata.first;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dario
 */
public class FirstReducer extends Reducer<Text, IntWritable, Text, Text>{
    public void reduce(Text inputKey, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] item = inputKey.toString().split(" ");
        int v = 0;
        while (values.hasNext()) {
            v = values.next().get();
        }
        String date = item[0];
        String value = item[1] + " " + v + ", ";
        
        context.write(new Text(date), new Text(value));
    }
}
