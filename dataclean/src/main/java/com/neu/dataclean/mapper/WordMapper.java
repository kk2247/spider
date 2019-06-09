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
 * @Date: 2019/6/9 0009 15:37
 * @Version 1.8
 */
public class WordMapper extends Mapper<LongWritable, Text,Text,IntWritable> {
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
        house.setTags(house.getTags().replace("[","").replace("]","")
                .replace("\"","").replace(" ","").trim());
        String[] tags=house.getTags().trim().split(",");
        String dis=house.getDis().trim();
        for(String tag:tags){
            if(tag.equals("")){
                return ;
            }
            context.write(new Text(dis+"#"+tag),new IntWritable(1));
        }
    }
}
