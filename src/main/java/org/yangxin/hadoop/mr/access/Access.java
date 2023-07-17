package org.yangxin.hadoop.mr.access;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂数据类型
 * <p>
 * 1）按照Hadoop的规范，需要实现Writable接口
 * 2）按照Hadoop的规范，需要实现write和readFields这两个方法
 * 3）定义一个默认的构造方法
 *
 * @author yangxin
 * 2023/7/16 22:33
 */
@Data
@NoArgsConstructor
public class Access implements Writable {

    private String phone;

    private long up;

    private long down;

    private long sum;

    public Access(String phone, long up, long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }
}
