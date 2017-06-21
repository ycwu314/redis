package com.example.redis.service;

import com.example.redis.RedisApplicationTests;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.BoundValueOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Created by Administrator on 2017/6/16 0016.
 */

public class TestRedis extends RedisApplicationTests {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisTemplate redisTemplate;

    @Test
    public void ping() {
        final String val = "aaaa";
        BoundValueOperations<String, String> ops = stringRedisTemplate.boundValueOps("TEST:AA");
        ops.set(val);
        ops.expire(1, TimeUnit.SECONDS);
        Assert.assertEquals(val, ops.get());
    }

}
