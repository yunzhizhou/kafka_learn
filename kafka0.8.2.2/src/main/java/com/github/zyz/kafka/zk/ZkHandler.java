package com.github.zyz.kafka.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by zyz
 * 18/4/25 14:02
 *
 * 删除zk节点用
 *
 */
public class ZkHandler {

    private final CuratorFramework curatorFramework;

    public ZkHandler() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.builder().connectString("202.204.71.10:2181/")
                .sessionTimeoutMs(10000).retryPolicy(retryPolicy).build();
        curatorFramework.start();
    }

    public void deleteNode(String nodePath) {
        try {
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().withVersion(-1).forPath(nodePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ZkHandler zkHandler = new ZkHandler();
        zkHandler.deleteNode("/kafka0.10.1.0");

    }
}
