package Q5;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Q5 implements Writable, DBWritable {

    private Integer id;
    private Integer year;
    private String name;
    private String artist;
    private Integer maxdelta;


    public Q5() {
    }

    public Q5(Integer id, Integer year, String name, String artist, Integer maxdelta) {
        this.id = id;
        this.year = year;
        this.name = name;
        this.artist = artist;
        this.maxdelta = maxdelta;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    public Integer getMaxdelta() {
        return maxdelta;
    }

    public void setMaxdelta(Integer maxdelta) {
        this.maxdelta = maxdelta;
    }

    @Override
    public String toString() {
        return "Q1{" +
                "id=" + id +
                ", year=" + year +
                ", genre='" + name + '\'' +
                ", artist='" + artist + '\'' +
                ", maxdelta=" + maxdelta +
                '}';
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.artist);
        dataOutput.writeInt(this.maxdelta);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.name = dataInput.readUTF();
        this.artist = dataInput.readUTF();
        this.maxdelta = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, this.id);
        preparedStatement.setInt(2, this.year);
        preparedStatement.setString(3, this.name);
        preparedStatement.setString(4, this.artist);
        preparedStatement.setInt(5, this.maxdelta);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.year = resultSet.getInt(2);
        this.name = resultSet.getString(3);
        this.name = resultSet.getString(4);
        this.maxdelta = resultSet.getInt(5);
    }
}
