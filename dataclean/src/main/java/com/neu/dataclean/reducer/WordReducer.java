package com.neu.dataclean.reducer;

import com.neu.dataclean.entity.Word;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 15:38
 * @Version 1.8
 */
public class WordReducer extends Reducer<Text, IntWritable,Word, Word> {

    private Map<String,Integer> map=new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int number=0;
        for(IntWritable v:values){
            number++;
        }
        String line=key.toString();
        String dis=line.split("#")[0];
        String tag=line.split("#")[1];
        if(tag.equals("")){
            return ;
        }
        Word word=new Word();
        word.setDis(dis);
        word.setTag(tag);
        word.setNumber(number);
        context.write(word,null);
    }
}
