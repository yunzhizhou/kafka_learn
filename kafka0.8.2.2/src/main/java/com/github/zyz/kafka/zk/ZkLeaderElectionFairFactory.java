package com.github.zyz.kafka.zk;

import org.apache.zookeeper.ZooKeeper;

/**
 * @author zyz
 * @since 2018/5/27
 */
public class ZkLeaderElectionFairFactory {

    private ZkLeaderElectionFairFactory() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ZooKeeper zooKeeper;
        private String connectString;
        private int sessionTimeout;
        private String rootPath;
        private ElectionCallback callback;

        public Builder zookeeper(ZooKeeper zooKeeper) {
            this.zooKeeper = zooKeeper;
            return this;
        }

        public Builder connectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public Builder sessionTimeout(int sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public Builder rootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public Builder callback(ElectionCallback callback) {
            this.callback = callback;
            return this;
        }

        public ZkLeaderElectionFair buildWithZk() {
            return new ZkLeaderElectionFair(zooKeeper,rootPath,callback);
        }

        public ZkLeaderElectionFair build() {
            return new ZkLeaderElectionFair(connectString,sessionTimeout,rootPath,callback);
        }

    }
}
