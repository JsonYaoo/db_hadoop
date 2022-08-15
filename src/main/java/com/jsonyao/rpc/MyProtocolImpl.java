package com.jsonyao.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * 自定义 HDFS RPC 实现类
 *
 * @author yaocs2
 * @since 2022-08-15
 */
public class MyProtocolImpl implements MyProtocol {

    public String hello(String name) {
        System.out.println("我被调用了...");
        return "hello " + name;
    }

    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature();
    }
}
