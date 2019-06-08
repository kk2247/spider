package com.neu.dataclean.mapper;
import com.neu.dataclean.entity.House;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.alibaba.fastjson.JSON;

import java.io.IOException;

/**
 * @author Administrator
 */
public class DataMapper extends Mapper<LongWritable, Text, IntWritable,House> {
    private static int id=0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        id++;
        String line=value.toString();
        if(line.trim().equals("")){
            return ;
        }
        House house = JSON.parseObject(line,House.class);
        if(house==null){
            return ;
        }
        context.write(new IntWritable(id),house);
    }
}
