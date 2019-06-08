package com.neu.dataclean.reducer;

import com.neu.dataclean.entity.House;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.*;
import java.io.IOException;

/**
 * @author Administrator
 */
public class DataReducer extends Reducer<IntWritable, House,House,House> {
    @Override
    protected void reduce(IntWritable key, Iterable<House> values, Context context) throws IOException, InterruptedException {
        int id= Integer.parseInt(key.toString());
        for(House house:values){
            house.setArea(house.getArea().replace("m²",""));
            String floor=house.getFloor();
            if(floor.contains("(") && floor.contains(")")){
                house.setFloor(floor.split("\\(")[0]);
            }
            house.setDate(house.getDate().replace("年建造",""));
            house.setTags(house.getTags().replace("[","").replace("]","")
                    .replace("\"",""));
            context.write(house, null);
        }


    }
}
