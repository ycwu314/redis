package com.example.redis.service.pubsub;

import com.example.redis.Constants;
import com.example.redis.RedisApplicationTests;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * Created by Administrator on 2017/6/16 0016.
 */
@Slf4j
public class TestPubSub extends RedisApplicationTests {

    @Autowired
    private RedisMessageListenerContainer redisMessageListenerContainer;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * <pre>
     * redis pub sub does not provide message persistence.
     * consumer will miss messages before it starts.
     * </pre>
     */
    @Test
    public void testConsumerMissMessagesBeforeStart() throws InterruptedException {

        Thread producer = null;
        boolean[] continueFlag = new boolean[]{true};
        try {
            // start producer
            AtomicInteger produceCounter = new AtomicInteger(0);
            producer = new Thread(
                new NumberProducer(continueFlag, produceCounter, stringRedisTemplate));
            producer.start();

            Thread.sleep(2000L);

            AtomicInteger consumeCounter = new AtomicInteger(0);
            MessageListenerAdapter messageListenerAdapter = new MessageListenerAdapter(
                new NumberConsumer(consumeCounter), "consume");
            messageListenerAdapter.afterPropertiesSet();
            // binding consumer
            redisMessageListenerContainer.addMessageListener(messageListenerAdapter,
                new ChannelTopic(Constants.TOPIC_SEND_NUMBER));

            Thread.sleep(2000L);

            int a = consumeCounter.get();
            int b = produceCounter.get();
            log.info("counter: consumer={}, producer={}", a, b);
            Assert.assertTrue(a < b);
        } finally {
            if (producer != null) {
                continueFlag[0] = false;
            }
        }
    }

    @Slf4j
    @NoArgsConstructor
    @AllArgsConstructor
    static class NumberProducer implements Runnable {

        private boolean[] continueFlag;
        private AtomicInteger counter;
        private StringRedisTemplate stringRedisTemplate;

        @Override
        public void run() {
            while (continueFlag[0]) {
                stringRedisTemplate
                    .convertAndSend(Constants.TOPIC_SEND_NUMBER,
                        String.valueOf(counter.incrementAndGet()));
                try {
                    Thread.sleep(300L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("exit");
        }
    }


    @Slf4j
    @NoArgsConstructor
    @AllArgsConstructor
    static class NumberConsumer {

        /**
         * consume message counter;
         */
        private AtomicInteger counter;

        public void consume(String message) {
            log.info("consumer messge=[{}]", message);
            counter.incrementAndGet();
        }
    }


}
