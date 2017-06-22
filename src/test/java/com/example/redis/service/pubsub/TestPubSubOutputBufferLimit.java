package com.example.redis.service.pubsub;

import com.example.redis.RedisApplicationTests;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

/**
 * Created by Administrator on 2017/6/21 0021.
 */
@Slf4j
public class TestPubSubOutputBufferLimit extends RedisApplicationTests {

    @Autowired
    private RedisProperties redisProperties;
    private JedisPool jedisPool;
    /**
     * default jedis pool size is 8, if want to change thread size, change pool size as well.
     */
    private static final int PUBLISH_THREADS = 3;
    private static final int SUBSCRIBE_THREADS = 1;

    private final String TEST_CHANNEL = "TEST_CHANNEL";
    private boolean[] continueFlag = new boolean[]{true};
    private int[] blockingConsumerTag = new int[]{0};

    private ExecutorService executorService = Executors.newCachedThreadPool();
    private CountDownLatch countDownLatch;

    @Before
    public void init() {
        jedisPool = new JedisPool(redisProperties.getHost(), redisProperties.getPort());
        countDownLatch = new CountDownLatch(1);
    }

    @After
    public void destroy() {
        if (jedisPool != null) {
            jedisPool.close();
        }

    }

    private void initPublishThreads(AtomicInteger publishedCounter, String content) {
        IntStream.range(0, PUBLISH_THREADS).forEach(i -> {
            executorService.submit(() -> {
                Jedis jedis = jedisPool.getResource();

                try {
                    countDownLatch.await();
                    while (continueFlag[0]) {
                        jedis.publish(TEST_CHANNEL, content);
                        publishedCounter.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    jedis.close();
                }
            });
        });
    }

    private void initSubscriberThreads(AtomicInteger subscribeCounter, boolean wait) {
        IntStream.range(0, SUBSCRIBE_THREADS).forEach(i -> {
            executorService.execute(() -> {

                Jedis jedis = jedisPool.getResource();
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        subscribeCounter.incrementAndGet();
                        if (wait) {
                            try {
                                Thread.sleep(100L);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }, TEST_CHANNEL);
                // the jedis subscribe() is blocking operation, so never reaches here
                blockingConsumerTag[0]++;
            });

        });
    }

    @Test
    public void testNormalPubSub() throws InterruptedException {

        AtomicInteger publishedCounter = new AtomicInteger(0);
        AtomicInteger consumedCounter = new AtomicInteger(0);

        initSubscriberThreads(consumedCounter, false);
        Thread.sleep(1000L);
        initPublishThreads(publishedCounter, "a");

        countDownLatch.countDown();
        Thread.sleep(2000L);

        continueFlag[0] = false;
        // waiting to stop publisher
        Thread.sleep(1000L);

        log.info("publish={}, consume={}", publishedCounter.get(), consumedCounter.get());
        Assert.assertEquals(publishedCounter.get(), consumedCounter.get());
        Assert.assertEquals(0, blockingConsumerTag[0]);
    }


    /**
     * <code>
     *
     * [9528] 22 Jun 11:23:54.349 # Client id=6 addr=127.0.0.1:5993 fd=11 name= age=1 idle=0 flags=N
     * db=0 sub=1 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=10 omem=1057728 events=rw
     * cmd=subscribe scheduled to be closed ASAP for overcoming of output buffer limits.
     *
     * </code>
     *
     * <code> redis.conf: client-output-buffer-limit pubsub 1mb 8mb 60  </code>
     */
    @Test
    public void testPubSubExceedBufferLimit() throws InterruptedException {

        AtomicInteger publishedCounter = new AtomicInteger(0);
        AtomicInteger consumedCounter = new AtomicInteger(0);

        initSubscriberThreads(consumedCounter, true);
        Thread.sleep(1000L);
        String content = buildBigContent(1024 * 128);
        initPublishThreads(publishedCounter, content);

        countDownLatch.countDown();
        Thread.sleep(2000L);

        continueFlag[0] = false;
        // waiting to stop publisher
        Thread.sleep(1000L);

        log.info("publish={}, consume={}", publishedCounter.get(), consumedCounter.get());
    }

    private String buildBigContent(int length) {
        Random random = new Random();
        byte[] b = new byte[length];
        random.nextBytes(b);

        return Base64.getEncoder().encodeToString(b);

    }

}
