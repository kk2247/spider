package com.neu.dataclean.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 15:37
 * @Version 1.8
 */
public class WordMapper extends Mapper<LongWritable, Text,Text,Integer> {
    
}
