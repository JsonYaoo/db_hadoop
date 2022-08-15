package com.jsonyao.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 客户端代码
 *
 * @author yaocs2
 * @since 2022-08-15
 */
public class MyClient {

    public static void main(String[] args) throws IOException {
        InetSocketAddress addr = new InetSocketAddress("localhost", 1234);
        Configuration conf = new Configuration();
        MyProtocol proxy = RPC.getProxy(MyProtocol.class, MyProtocol.versionID, addr, conf);
        String result = proxy.hello("RPC");
        System.out.println("RPC Client revived: " + result);
    }
}
