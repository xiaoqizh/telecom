package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:47 2018/7/1
 * @Description:
 */

public class DateDimension extends BaseDimension {
    /**
     * 如果month 为 -1 那么就查询整年的
     */
    private String year;
    private String month;
    private String day;
    public DateDimension() {
        super();
    }

    public DateDimension(String year, String month, String day) {
        super();
        this.year = year;
        this.month = month;
        this.day = day;
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension another = (DateDimension) o;

        return  this.year.compareTo(another.year) == 0 ?
                (this.month.compareTo(another.month) == 0 ?
                 this.day.compareTo(another.day)
                : this.month.compareTo(another.month))
                : this.year.compareTo(another.year);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.year);
        dataOutput.writeUTF(this.month);
        dataOutput.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        if (month != null ? !month.equals(that.month) : that.month != null) return false;
        return day != null ? day.equals(that.day) : that.day == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        return result;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}
