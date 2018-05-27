package com.github.zyz.kafka.zk;

/**
 * @author zyz
 * @since 2018/5/27
 */
public interface ElectionCallback {

    void becomeLeader(String leaderPath);

}
