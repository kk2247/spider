package com.neu.dataclean.entity;
import com.alibaba.fastjson.annotation.JSONField;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class House implements Serializable, Writable, DBWritable {

    private static final long serialVersionUID = 1L;

    private String title;

    private String room;

    private String area;

    private String floor;

    private String date;

    private String location;

    private float detPrice;

    private float unitPrice;

    private String tags;

    private String dis;

    @JSONField(name = "title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @JSONField(name = "room")
    public String getRoom() {
        return room;
    }

    public void setRoom(String room) {
        this.room = room;
    }

    @JSONField(name = "area")
    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    @JSONField(name = "floor")
    public String getFloor() {
        return floor;
    }

    public void setFloor(String floor) {
        this.floor = floor;
    }

    @JSONField(name = "date")
    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @JSONField(name = "location")
    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @JSONField(name = "det_price")
    public float getDetPrice() {
        return detPrice;
    }

    public void setDetPrice(String detPrice) {
        if(detPrice.contains("万")){
            this.detPrice=Float.valueOf(detPrice.split("万")[0]);
        }else{
            this.detPrice=Float.valueOf(detPrice);
        }
    }

    @JSONField(name = "unit_price")
    public float getUnitPrice() {
        return unitPrice;
    }


    public void setUnitPrice(String unitPrice) {
        if(unitPrice.contains("元/m²")){
            this.unitPrice=Float.valueOf(unitPrice.split("元/m²")[0]);
        }else{
            this.unitPrice=Float.valueOf(unitPrice);
        }
    }

    @JSONField(name = "tags")
    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @JSONField(name = "dis")
    public String getDis() {
        return dis;
    }

    public void setDis(String dis) {
        this.dis = dis;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.title);
        dataOutput.writeUTF(this.room);
        dataOutput.writeUTF(this.area);
        dataOutput.writeUTF(this.floor);
        dataOutput.writeUTF(this.date);
        dataOutput.writeUTF(this.location);
        dataOutput.writeFloat(this.detPrice);
        dataOutput.writeFloat(this.unitPrice);
        dataOutput.writeUTF(this.tags);
        dataOutput.writeUTF(this.dis);


    }

    @Override
    public String toString() {
        return "House{" +
                "title='" + title + '\'' +
                ", room='" + room + '\'' +
                ", area='" + area + '\'' +
                ", floor='" + floor + '\'' +
                ", date='" + date + '\'' +
                ", location='" + location + '\'' +
                ", detPrice=" + detPrice +
                ", unitPrice=" + unitPrice +
                ", tags='" + tags + '\'' +
                ", dis='" + dis + '\'' +
                '}';
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.title=dataInput.readUTF();
        this.room=dataInput.readUTF();
        this.area=dataInput.readUTF();
        this.floor=dataInput.readUTF();
        this.date=dataInput.readUTF();
        this.location=dataInput.readUTF();
        this.detPrice=dataInput.readFloat();
        this.unitPrice=dataInput.readFloat();
        this.tags=dataInput.readUTF();
        this.dis=dataInput.readUTF();
    }
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,this.title);
        preparedStatement.setString(2,this.room);
        preparedStatement.setString(3,this.area);
        preparedStatement.setString(4,this.floor);
        preparedStatement.setString(5,date);
        preparedStatement.setString(6,location);
        preparedStatement.setFloat(7,this.detPrice);
        preparedStatement.setFloat(8,this.unitPrice);
        preparedStatement.setString(9,this.tags);
        preparedStatement.setString(10,dis);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        title=resultSet.getString(1);
        room=resultSet.getString(2);
        area=resultSet.getString(3);
        floor=resultSet.getString(4);
        date=resultSet.getString(5);
        location=resultSet.getString(6);
        detPrice=resultSet.getFloat(7);
        unitPrice=resultSet.getFloat(8);
        tags=resultSet.getString(9);
        dis=resultSet.getString(10);
    }
}
