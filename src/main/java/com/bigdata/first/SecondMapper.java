package com.bigdata.first;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dario
 */
public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private final IntWritable one = new IntWritable(1);
    
    @Override
    public void map(LongWritable inputKey, Text inputValue, Mapper.Context context) throws InterruptedException, IOException {
        String[] lines = inputValue.toString().split("\n");
        for(String line : lines){
            String[] splittedLine = line.split(",");
            String month = splittedLine[0].substring(0,7);
            for(int i = 1; i < splittedLine.length; i++){
                context.write(new Text(month + " " + splittedLine[i]), one);
            }
        }
    }
}
