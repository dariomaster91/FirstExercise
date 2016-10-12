package com.bigdata.first;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dario
 */
public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws InterruptedException, IOException {
        
        String[] lines = inputValue.toString().split("\n");
        String month = "", list, result = "";
        
        for (String line : lines) {
            month = line.substring(0, line.indexOf("\t"));
            list = line.substring(line.indexOf("\t") + 1);
            String[] splittedLine = list.split(",");
            HashMap<String, Integer> m = new HashMap();

            for (String record : splittedLine) {
                String food = record.trim().substring(0, record.indexOf("\t"));
                String value = record.trim().substring(record.indexOf("\t") + 1);
                m.put(food, Integer.parseInt(value));
            }
            
            // Sort HashMap by value
            
            Object[] a = m.entrySet().toArray();
            Arrays.sort(a, (Object o1, Object o2) -> ((Map.Entry<String, Integer>) o2).getValue().compareTo(((Map.Entry<String, Integer>) o1).getValue()));
            for (Object e : a) {
                result += ((Map.Entry<String, Integer>) e).getKey() + " " + ((Map.Entry<String, Integer>) e).getValue() + ", ";
            }
            
        }
        
        // Limit output number to 5 and trim final character ","
        
        String[] tokens = result.split(",");
        if (tokens.length <= 5) {
            context.write(new Text(month + ":"), new Text(result.substring(0, result.length() - 2)));
        } else {
            result = "";
            for (int i = 0; i < 4; i++) {
                result += tokens[i] + ",";
            }
            result += tokens[5];
            context.write(new Text(month + ":"), new Text(result));
        }
    }
}
