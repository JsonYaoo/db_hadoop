package com.jsonyao.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 自定义 HDFS RPC 接口
 *
 * @author yaocs2
 * @since 2022-08-15
 */
public interface MyProtocol extends VersionedProtocol {

    long versionID = 123456;

    /**
     * 自定义方法
     *
     * @param name
     * @return
     */
    String hello(String name);
}
