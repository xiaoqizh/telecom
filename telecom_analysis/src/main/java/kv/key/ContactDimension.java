package kv.key;

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:39 2018/7/1
 * @Description: 联系人维度
 */

public class ContactDimension extends BaseDimension {
    private String telephone;
    private String name;

    public ContactDimension() {
        super();
    }

    public ContactDimension(String telephone, String name) {
        super();
        this.telephone = telephone;
        this.name = name;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension another = (ContactDimension) o;
        return this.name.compareTo(another.name) != 0
                ? this.name.compareTo(another.name)
                : this.telephone.compareTo(another.telephone);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(    this.telephone);
        dataOutput.writeUTF(    this.name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.telephone = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContactDimension that = (ContactDimension) o;

        if (telephone != null ? !telephone.equals(that.telephone) : that.telephone != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = telephone != null ? telephone.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
