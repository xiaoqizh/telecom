package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:49 2018/7/1
 * @Description:
 */

public class CommonDimension extends BaseDimension {

    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();

    public CommonDimension() {
    }

    @Override
    public int compareTo(BaseDimension o) {
        CommonDimension c = (CommonDimension) o;
        return  this.dateDimension.compareTo(c.getDateDimension()) == 0
                ? this. contactDimension.compareTo(c.getContactDimension())
                : dateDimension.compareTo(c.getDateDimension());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        contactDimension.write(dataOutput);
        dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contactDimension.readFields(dataInput);
        dateDimension.readFields(dataInput);
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }
}
