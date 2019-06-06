package com.neu.dataclean.driver;

import com.neu.dataclean.mapper.DataMapper;
import com.neu.dataclean.reducer.DataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class DataDriver {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:/hadoop-2.9.2");
        if (args == null || args.length == 0) {
            return;
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDriver.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String in="";
        String out="";
        FileInputFormat.addInputPath(job,new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.waitForCompletion(true);
    }
}
