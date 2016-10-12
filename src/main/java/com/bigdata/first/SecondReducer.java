package com.bigdata.first;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dario
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text inputKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] item = inputKey.toString().split(" ");
        Integer v = 0;
        while (values.iterator().hasNext()) {
            v = Integer.parseInt(values.iterator().next().toString());
        }
        String date = item[0];
        String value = item[1] + " " + v + ", ";
        
        context.write(new Text(date), new Text(value));
    }
}
