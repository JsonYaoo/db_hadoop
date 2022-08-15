package com.jsonyao.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * 服务端代码
 *
 * @author yaocs2
 * @since 2022-08-15
 */
public class MyServer {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        RPC.Builder rpcBuilder = new RPC.Builder(conf);
        rpcBuilder.setBindAddress("localhost")
                .setPort(1234)
                .setProtocol(MyProtocol.class)
                .setInstance(new MyProtocolImpl());

        RPC.Server rpcServer = rpcBuilder.build();
        rpcServer.start();
        System.out.println("RPC Server start...");
    }
}
