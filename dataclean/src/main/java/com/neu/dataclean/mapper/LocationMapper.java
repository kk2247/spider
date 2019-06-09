package com.neu.dataclean.mapper;

import com.alibaba.fastjson.JSON;
import com.neu.dataclean.entity.House;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 14:37
 * @Version 1.8
 */
public class LocationMapper extends Mapper<LongWritable, Text,Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        if(line.trim().equals("")){
            return ;
        }
        House house = JSON.parseObject(line,House.class);
        if(house==null){
            return ;
        }
        context.write(new Text(house.getDis()),new FloatWritable(house.getUnitPrice()));
    }
}
