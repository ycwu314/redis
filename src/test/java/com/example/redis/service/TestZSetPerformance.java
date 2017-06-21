package com.example.redis.service;

import com.example.redis.RedisApplicationTests;
import java.io.IOException;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.BoundZSetOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

/**
 * Created by Administrator on 2017/6/19 0019.
 */
@Slf4j
public class TestZSetPerformance extends RedisApplicationTests {

    @Autowired
    private RedisTemplate redisTemplate;
    @Autowired
    private RedisConnectionFactory redisConnectionFactory;


    private Random random = new Random();

    private static final int MAX_SIZE = 100_000;
    private static final int BATCH_SIZE = 1000;
    final String TEST_ZSET_KEY = "TEST:ZSET";


    /**
     * <pre>
     * really slow:
     * jedis cluster DOES NOT support pipeline, redisTemplate.execute(new RedisCallback() {} will
     * throw exception.
     *
     * spring data redis default serializer(JDK serializer) cost much space, at least change to use
     * GenericToStringSerializer
     *
     * </pre>
     */
    @Test
    public void testInsertLargeZSet() {
        redisTemplate.setKeySerializer(new GenericToStringSerializer<>(Integer.class));
        redisTemplate.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
        redisTemplate.afterPropertiesSet();
        BoundZSetOperations<Integer, Integer> ops = redisTemplate.boundZSetOps(TEST_ZSET_KEY);

        log.info("start prepare data");
        long start = System.currentTimeMillis();
        for (int i = 0; i < MAX_SIZE; i++) {
            ops.add(i, (double) random.nextFloat() * 100_0000);
        }
        long end = System.currentTimeMillis();
        log.info("end prepare data; cost around {} second", (end - start) / 1000);


    }

    @Test
    public void testReadFromZSet() {
        redisTemplate.setKeySerializer(new GenericToStringSerializer<>(Integer.class));
        redisTemplate.setValueSerializer(new GenericToStringSerializer<>(Integer.class));
        redisTemplate.afterPropertiesSet();
        BoundZSetOperations<Integer, Integer> ops = redisTemplate.boundZSetOps(TEST_ZSET_KEY);
        // really fast
        ops.rangeWithScores(90000, 90050);
    }


    /**
     * get the redis node of a particular key. then connect to the very node using jedis directly
     */
    @Test
    public void testInsertLargeZSet2() {
        RedisClusterNode node = redisConnectionFactory.getClusterConnection()
            .clusterGetNodeForKey(TEST_ZSET_KEY.getBytes());

        Jedis jedis = null;

        try {
            jedis = new Jedis(node.getHost(), node.getPort());

            jedis.del(TEST_ZSET_KEY);
            log.info("clean old key={}", TEST_ZSET_KEY);

            for (int i = 0; i < MAX_SIZE; i += BATCH_SIZE) {
                int end = (i + BATCH_SIZE) <= MAX_SIZE ? (i + BATCH_SIZE) : MAX_SIZE;
                log.info("batch=[{},{}]", i, end);

                Pipeline pipeline = jedis.pipelined();
                for (int j = i; j < end; j++) {
                    pipeline
                        .zadd(TEST_ZSET_KEY, (double) random.nextFloat() * 100_0000,
                            String.valueOf(i));
                }
                pipeline.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

    }


}
