package com.neu.dataclean.entity;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 14:56
 * @Version 1.8
 */
public class Location implements DBWritable {
    private String dis;

    private int size;

    private float unitPrice;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getDis() {
        return dis;
    }

    public void setDis(String dis) {
        this.dis = dis;
    }

    public float getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(float unitPrice) {
        this.unitPrice = unitPrice;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,dis);
        preparedStatement.setInt(2,size);
        preparedStatement.setFloat(3,unitPrice);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
