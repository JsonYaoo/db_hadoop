package com.jsonyao.mr;

import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * 测试Hadoop序列化机制
 *
 * @author yaocs2
 * @since 2022-08-18
 */
public class HadoopSerialize {
    public static void main(String[] args) throws IOException {
        StudentWritable studentWritable = new StudentWritable();
        studentWritable.setId(1L);
        studentWritable.setName("Hadoop");

        // 写入本地文件 => 22字节, 能节省接近10倍的存储空间
        FileOutputStream fos = new FileOutputStream("D:\\Users\\yaocs2\\data\\myWorkspace\\db_hadoop\\src\\main\\resources\\student_hadoop.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        studentWritable.write(oos);
        oos.close();
        fos.close();
    }
}

class StudentWritable implements Writable {

    private Long id;
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = in.readUTF();
    }
}
