package com.example.redis.service;

import com.example.redis.RedisApplicationTests;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.BoundValueOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Created by Administrator on 2017/6/20 0020.
 */
@Slf4j
public class TestDistributeLock extends RedisApplicationTests {

    @Autowired
    private RedisTemplate redisTemplate;
    final String KEY = "TEST:DEADLOCK";
    private static final int THREAD_SIZE = 5;


    private final long TIMEOUT_MS = 1000L;

    @Before
    public void before() {
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();

        // clean data
        redisTemplate.delete(KEY);
        log.info("clean key={}", KEY);
    }

    /**
     * Setnx is not enough. Caused dead lock if acquire the lock then died before setting timeout.
     * DO NOT use it.
     */
    @Test
    public void testDeadLock() {

        final int NOBODY = -1;

        int[] firstLockWinner = new int[]{NOBODY};
        int[] secondLockWinner = new int[]{NOBODY};
        boolean[] continueFlag = new boolean[]{true};

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // start threads
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            IntStream.range(0, THREAD_SIZE).forEach(i -> {
                executorService.submit(() -> {
                    BoundValueOperations<String, String> ops = redisTemplate.boundValueOps(KEY);
                    try {
                        countDownLatch.await();

                        boolean lock = false;
                        while (!lock && continueFlag[0]) {
                            log.info("thread {} trying to get lock", i);
                            lock = ops
                                .setIfAbsent(buildLockValue());
                            if (lock) {
                                if (firstLockWinner[0] == NOBODY) {
                                    firstLockWinner[0] = i;
                                } else {
                                    // hehe, never reach here
                                    secondLockWinner[0] = i;
                                }

                                log.info("************* thread {} got the lock", i);
                                // simulate the winner goes wrong and cant lock release the lock
                                log.info("************* thread {} goes crazy", i);
                                throw new RuntimeException("thread " + i + " goes crazy");
                            }

                            Thread.sleep(1000L);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });

            countDownLatch.countDown();

            try {
                Thread.sleep(10000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Assert.assertTrue(firstLockWinner[0] != NOBODY);
            Assert.assertEquals(secondLockWinner[0], NOBODY);
        } finally {
            continueFlag[0] = false;
            executorService.shutdown();
        }

    }

    /**
     * try getset the lock to avoid multiple update
     * @throws InterruptedException
     */
    @Test
    public void testDistributedLock() throws InterruptedException {

        // clean data
        final String KEY_JOB_DONE = "TEST:JOB_DONE";
        redisTemplate.delete(KEY_JOB_DONE);

        final long EXIT_TIMEOUT = System.currentTimeMillis() + 3 * TIMEOUT_MS;
        AtomicInteger acquireLockCounter = new AtomicInteger(0);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        // start threads
        ExecutorService executorService = Executors.newCachedThreadPool();
        IntStream.range(0, THREAD_SIZE).forEach(i -> {

            executorService.submit(() -> {
                BoundValueOperations<String, String> ops = redisTemplate.boundValueOps(KEY);

                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String ret = null;

                while (System.currentTimeMillis() < EXIT_TIMEOUT
                    && redisTemplate.boundValueOps(KEY_JOB_DONE).get() == null) {

                    if (ops.setIfAbsent(buildLockValue())) {
                        log.info("thread {} acquires the lock", i);
                        int count = acquireLockCounter.incrementAndGet();

                        // for demo purpose, let count=1 thread died
                        if (count == 1) {
                            log.info("### thread {} goes wrong and does not release the lock", i);
                            throw new RuntimeException();
                        }


                    } else {
                        long currentTimeMillis = System.currentTimeMillis();
                        ret = ops.get();
                        log.info("thread {} lock expiration={}, current timestamp={}", i, ret,
                            currentTimeMillis);
                        if (currentTimeMillis < Long.valueOf(ret)) {
                            try {
                                // sleep randomly
                                Thread.sleep((long) (Math.random() * 100));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            // lock expiration
                            String oldLockVal = ret;
                            ret = ops.getAndSet(buildLockValue());
                            // double check got the lock
                            if (oldLockVal.equals(ret)) {
                                // do sth
                                log.info("thread {} acquires the lock and does sth", i);

                                redisTemplate.boundValueOps(KEY_JOB_DONE)
                                    .set(String.valueOf(System.currentTimeMillis()), TIMEOUT_MS,
                                        TimeUnit.MICROSECONDS);

                                redisTemplate.expire(KEY, TIMEOUT_MS, TimeUnit.MICROSECONDS);
                            }
                        }
                    }
                }


            });

        });

        countDownLatch.countDown();

        Thread.sleep(5000L);


    }

    private String buildLockValue() {
        return String.valueOf(System.currentTimeMillis() + TIMEOUT_MS + 1);
    }


}

