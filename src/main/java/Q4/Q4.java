package Q4;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Q4 implements Writable, DBWritable {

    private Integer id;
    private Integer year;
    private String genre;
    private Integer frequency;


    public Q4() {
    }

    public Q4(Integer id, Integer year, String genre, Integer frequency) {
        this.id = id;
        this.year = year;
        this.genre = genre;
        this.frequency = frequency;
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

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "Q1{" +
                "id=" + id +
                ", year=" + year +
                ", genre='" + genre + '\'' +
                ", frequency=" + frequency +
                '}';
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeUTF(this.genre);
        dataOutput.writeInt(this.frequency);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.genre = dataInput.readUTF();
        this.frequency = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, this.id);
        preparedStatement.setInt(2, this.year);
        preparedStatement.setString(3, this.genre);
        preparedStatement.setInt(4, this.frequency);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getInt(1);
        this.year = resultSet.getInt(2);
        this.genre = resultSet.getString(3);
        this.frequency = resultSet.getInt(4);
    }
}
