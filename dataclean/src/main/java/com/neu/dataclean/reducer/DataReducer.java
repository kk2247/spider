package com.neu.dataclean.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.*;
import java.io.IOException;

/**
 * @author Administrator
 */
public class DataReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text:values){
            String line=text.toString();
            String[] attributes=line.split("u0001");
            StringBuilder stringBuilder=new StringBuilder();
            stringBuilder.append(attributes[1].trim()+"#");
            stringBuilder.append(attributes[2].trim()+"#");
            stringBuilder.append(attributes[3].substring(0,attributes[3].length()-1)+"#");
            String floor = attributes[4].trim();
            if(floor.contains("(") && floor.contains(")")){
                floor=floor.substring(0,1);
            }else{
                floor="全";
            }
            stringBuilder.append(floor+"#");
            stringBuilder.append(attributes[5].trim().replace("年建造","")+"#");
            stringBuilder.append(attributes[6].trim()+"#");
            stringBuilder.append(attributes[7].trim()+"#");
            stringBuilder.append(attributes[8].trim()+"#");
            stringBuilder.append(attributes[9].trim().replace("[","").replace("]","")+"#");
            stringBuilder.append(attributes[10].trim());
            context.write(key,new Text(stringBuilder.toString()));
        }
    }
}
