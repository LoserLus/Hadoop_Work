package Q2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Q2 implements Writable, DBWritable {

    private Integer id;
    private Integer year;
    private Integer weeksoncharts;
    private String artist;



    public Q2() {
    }

    public Q2(Integer id, Integer year, Integer weeksoncharts, String artist) {
        this.id = id;
        this.year = year;
        this.weeksoncharts = weeksoncharts;
        this.artist = artist;
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

    public Integer getWeeksoncharts() {
        return weeksoncharts;
    }

    public void setWeeksoncharts(Integer weeksoncharts) {
        this.weeksoncharts = weeksoncharts;
    }

    public String getArtist() {
        return artist;
    }

    public void setArtist(String artist) {
        this.artist = artist;
    }

    @Override
    public String toString() {
        return "Q1{" +
                "id=" + id +
                ", year=" + year +
                ", weeksoncharts=" + weeksoncharts +
                ", artist='" + artist + '\'' +
                '}';
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.weeksoncharts);
        dataOutput.writeUTF(this.artist);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.weeksoncharts = dataInput.readInt();
        this.artist = dataInput.readUTF();

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, this.id);
        preparedStatement.setInt(2, this.year);
        preparedStatement.setInt(3, this.weeksoncharts);
        preparedStatement.setString(4, this.artist);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.year = resultSet.getInt(2);
        this.weeksoncharts = resultSet.getInt(3);
        this.artist = resultSet.getString(4);

    }
}
