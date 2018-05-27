package com.github.zyz.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author zyz
 * @since 2018/5/25
 *
 * Hash分配
 */
public class HashPartitioner implements Partitioner {

    public HashPartitioner(VerifiableProperties verifiableProperties) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        if ((key instanceof Integer)) {
            return Math.abs(Integer.parseInt(key.toString())) % numPartitions;
        }
        return Math.abs(key.hashCode() % numPartitions);
    }
}
