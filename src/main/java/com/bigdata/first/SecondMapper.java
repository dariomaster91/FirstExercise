package com.bigdata.first;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author dario
 */
public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws InterruptedException, IOException {
        String[] lines = inputValue.toString().split("\n");
        for(String line : lines){
            String[] splittedLine = line.split(" ");
            for(int i = 1; i < splittedLine.length; i++){
                context.write(new Text(splittedLine[0]), new Text(splittedLine[i]));
            }
        }
    }
}
