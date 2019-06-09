package com.neu.dataclean.driver;

import com.neu.dataclean.entity.House;
import com.neu.dataclean.entity.Word;
import com.neu.dataclean.mapper.DataMapper;
import com.neu.dataclean.mapper.LocationMapper;
import com.neu.dataclean.mapper.WordMapper;
import com.neu.dataclean.reducer.DataReducer;
import com.neu.dataclean.reducer.LocationReducer;
import com.neu.dataclean.reducer.WordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class DataDriver {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:/hadoop-2.9.2");
        if (args == null || args.length == 0) {
            return;
        }
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/mydb?useUnicode=true&characterEncoding=utf8&useSSL=false", "root", "root");
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDriver.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(House.class);
        job.setOutputKeyClass(House.class);
        job.setOutputValueClass(House.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "tabhouse", "title", "room",
                "area","floor","mdate","location","det_price","unit_price","tags","dis");
        job.addArchiveToClassPath(new Path("lib/mysql-connector-java-5.1.46.jar"));
        ControlledJob controlledJob=new ControlledJob(conf);
        controlledJob.setJob(job);

        Job job1=Job.getInstance(conf,"unit price in area");
        job1.setJarByClass(DataDriver.class);
        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(WordReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(DBOutputFormat.class);

        job1.addArchiveToClassPath(new Path("lib/mysql-connector-java-5.1.46.jar"));
        DBOutputFormat.setOutput(job1, "tag_area", "dis","tag","number");
        ControlledJob controlledJob1=new ControlledJob(conf);
        controlledJob1.setJob(job1);
//        controlledJob1.addDependingJob(controlledJob);



        String in="input/";
        FileInputFormat.addInputPath(job, new Path(in));
        FileInputFormat.addInputPath(job1, new Path(in));

        JobControl ctrl=new JobControl("all job");
        ctrl.addJob(controlledJob);
        ctrl.addJob(controlledJob1);

        Thread  t=new Thread(ctrl);
        t.start();

        while(true){
            if(ctrl.allFinished()){
                System.out.println(ctrl.getSuccessfulJobList());
                ctrl.stop();
                break;
            }
        }
    }
}
