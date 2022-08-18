package com.jsonyao.mr;

import java.io.*;

/**
 * 测试Java序列化机制
 *
 * @author yaocs2
 * @since 2022-08-18
 */
public class JavaSerialize {
    public static void main(String[] args) throws IOException {
        StudentJava studentJava = new StudentJava();
        studentJava.setId(1L);
        studentJava.setName("Hadoop");

        // 写入本地文件 => 186字节
        FileOutputStream fos = new FileOutputStream("D:\\Users\\yaocs2\\data\\myWorkspace\\db_hadoop\\src\\main\\resources\\student_java.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(studentJava);
        oos.close();
        fos.close();
    }
}

class StudentJava implements Serializable {
    private static final long serialVersionUID = 1L;

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
}
