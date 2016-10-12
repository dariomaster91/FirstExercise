package com.bigdata.first;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author dario
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text inputKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = "";
        while (values.iterator().hasNext()) {
            value += values.iterator().next().toString() + ", ";
        }    
        context.write(inputKey, new Text(value));
    }
}
