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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author dario
 */
public class FirstDriver{
    public static void main(String[] args) throws Exception {
        
        JobControl jobControl = new JobControl("job-control");
        
        //  First Job section
        
        Job firstJob = Job.getInstance(new Configuration(), "first-job");
        firstJob.setJarByClass(FirstDriver.class);
        firstJob.setMapperClass(SecondMapper.class);
        //firstJob.setReducerClass(FirstReducer.class);
        firstJob.setMapOutputKeyClass(Text.class);
        firstJob.setMapOutputValueClass(IntWritable.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(IntWritable.class);
        ControlledJob controlledFirstJob = new ControlledJob(firstJob.getConfiguration());
        controlledFirstJob.setJob(firstJob);
        Date now = Calendar.getInstance().getTime();
        DateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
        String datetime = f.format(now);
        String outputPath = args[1] + datetime;
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(outputPath));    
        
        jobControl.addJob(controlledFirstJob);
        
        //  Second Job section
        
        /*Job secondJob = Job.getInstance(new Configuration(), "second-job");
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setReducerClass(SecondReducer.class);
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);
        String inputSecondJob = outputPath + "/part*";
        outputPath += "_final";
        FileInputFormat.addInputPath(secondJob, new Path(inputSecondJob));
        FileOutputFormat.setOutputPath(secondJob, new Path(outputPath));
        ControlledJob controlledSecondJob = new ControlledJob(firstJob.getConfiguration());
        controlledSecondJob.setJob(secondJob);
        controlledSecondJob.addDependingJob(controlledFirstJob);
        
        jobControl.addJob(controlledSecondJob);*/
        
        jobControl.run();
    }   
}
