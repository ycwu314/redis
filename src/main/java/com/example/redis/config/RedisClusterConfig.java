package com.example.redis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Administrator on 2017/6/16 0016.
 */
@Configuration
public class RedisClusterConfig {

    @Autowired
    private RedisProperties redisProperties;

    /**
     * <pre>
     * @Bean
     * public RedisConnectionFactory redisConnectionFactory() {
     *      JedisConnectionFactory factory = new JedisConnectionFactory(
     *          new RedisClusterConfiguration(redisProperties.getCluster().getNodes()));
     *      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
     *      // TODO set config
     *      return factory;
     * }
     * </pre>
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        JedisConnectionFactory factory = new JedisConnectionFactory(
            new RedisClusterConfiguration(redisProperties.getCluster().getNodes()));
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // TODO set config
        return factory;
    }


    @Bean
    @Autowired
    public RedisClusterConnection redisClusterConnection(
        RedisConnectionFactory redisConnectionFactory) {
        return redisConnectionFactory.getClusterConnection();
    }

    @Bean
    @Autowired
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        return new StringRedisTemplate(redisConnectionFactory);
    }

}
