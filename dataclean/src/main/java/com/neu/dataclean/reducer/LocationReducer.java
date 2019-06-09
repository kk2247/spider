package com.neu.dataclean.reducer;

import com.neu.dataclean.entity.Location;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 14:44
 * @Version 1.8
 */
public class LocationReducer extends Reducer<Text, FloatWritable, Location,Location> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0;
        int size=0;
        for(FloatWritable value:values){
            sum+=value.get();
            size++;
        }
        Location location=new Location();
        location.setDis(key.toString());
        location.setUnitPrice(sum/size);
        location.setSize(size);
        context.write(location,null);
    }
}
