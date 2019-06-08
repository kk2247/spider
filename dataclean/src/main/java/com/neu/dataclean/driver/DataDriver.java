package com.neu.dataclean.driver;

import com.neu.dataclean.entity.House;
import com.neu.dataclean.mapper.DataMapper;
import com.neu.dataclean.reducer.DataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        String in="input/";
        FileInputFormat.addInputPath(job, new Path(in));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
