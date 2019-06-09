package com.neu.dataclean.entity;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: KGZ
 * @Date: 2019/6/9 0009 17:23
 * @Version 1.8
 */
public class Word implements DBWritable {
    private String dis;

    private String tag;

    private int number;

    public String getDis() {
        return dis;
    }

    public void setDis(String dis) {
        this.dis = dis;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,dis);
        preparedStatement.setString(2,tag);
        preparedStatement.setInt(3,number);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }
}
