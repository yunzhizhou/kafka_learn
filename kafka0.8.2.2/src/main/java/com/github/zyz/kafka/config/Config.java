package com.github.zyz.kafka.config;

import com.typesafe.config.ConfigFactory;

import java.util.Objects;

/**
 * @author zyz
 * @since 2016/10/21
 */
public final class Config {

    private Config() {}

    private static com.typesafe.config.Config config = ConfigFactory.load();

    public static String standaloneBrokerList() {
        return getString("standalone.metadata.broker.list");
    }

    public static String clusterBrokerList() {
        return getString("cluster.metadata.broker.list");
    }

    private static String getString(String key) {
        return getString(key,"");
    }

    private static String getString(String key, String defaultValue) {
        Objects.requireNonNull(key,"Config key must not be null");
        return config.hasPath(key) ? config.getString(key) : defaultValue;
    }

}
