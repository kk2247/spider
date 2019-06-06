package com.neu.dataclean.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Administrator
 */
public class DataMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        if(line.trim().equals("")){
            return ;
        }
        String[] values=line.split("\u0001");
        if(value.getLength()!=11){
            return ;
        }
        String id=values[0].trim();
        context.write(new Text(id),new Text(line));
    }
}
