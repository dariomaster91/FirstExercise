/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bigdata.first;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dario
 */
public class FirstDriver{
    public static void main(String[] args) throws Exception {
        
        Job job = Job.getInstance(new Configuration(), "exercise1");
        
        job.setJarByClass(FirstDriver.class);
        
        job.setMapperClass(FirstMapper.class);
        job.setCombinerClass(FirstCombiner.class);
        job.setReducerClass(FirstReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        Date now = Calendar.getInstance().getTime();
        DateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
        String outputPath = args[1] + f.format(now);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }   
}
