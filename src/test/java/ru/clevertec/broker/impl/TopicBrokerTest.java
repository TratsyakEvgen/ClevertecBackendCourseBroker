package ru.clevertec.broker.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import ru.clevertec.broker.Broker;
import ru.clevertec.broker.client.Consumer;
import ru.clevertec.broker.client.Producer;
import ru.clevertec.broker.exception.BrokerException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TopicBrokerTest {

    private final static String TOPIC_NAME = "topic";

    private Broker broker;

    @BeforeEach
    void setUp() {
        broker = new TopicBroker();
        broker.register(TOPIC_NAME, new Semaphore(10, false));
    }

    @Test
    void register_ifTopicAlreadyExists() {
        assertThrows(BrokerException.class, () -> broker.register(TOPIC_NAME, new Semaphore(3, false)));
    }

    @Test
    void read_ifTopicNotExists() {
        assertThrows(BrokerException.class, () -> broker.read("some topic", 1));
    }

    @Test
    void write_ifTopicNotExists() {
        assertThrows(BrokerException.class, () -> broker.write("some topic", "some message"));
    }

    @RepeatedTest(100)
    void read_write_oneProducerAndManuConsumer() {
        int countConsumer = 100;
        List<CountDownLatch> latches = Stream.generate(() -> new CountDownLatch(1)).limit(countConsumer).toList();
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < countConsumer; i++) {
            consumers.add(new Consumer(i, TOPIC_NAME, broker, latches.get(i)));
        }

        consumers.stream().map(Thread::new).forEach(Thread::start);
        new Thread(new Producer("1", TOPIC_NAME, broker)).start();
        latches.forEach(countDownLatch -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });


        consumers.forEach(consumer -> assertEquals(List.of("1"), consumer.getMessages())
        );
    }


    @RepeatedTest(100)
    void read_write_ManyProducerAndOneConsumer() throws InterruptedException {
        int countProducer = 100;
        List<Producer> producers = new ArrayList<>();
        List<String> messages = new ArrayList<>();

        for (int i = 0; i < countProducer; i++) {

            String message = String.valueOf(i);
            producers.add(new Producer(message, TOPIC_NAME, broker));
            messages.add(message);
        }
        CountDownLatch latch = new CountDownLatch(countProducer);
        Consumer consumer = new Consumer(1, TOPIC_NAME, broker, latch);

        producers.stream().map(Thread::new).forEach(Thread::start);
        new Thread(consumer).start();
        latch.await();

        assertEquals(messages.stream().sorted().toList(), consumer.getMessages().stream().sorted().toList());
    }

    @RepeatedTest(100)
    void read_write_ManyProducerAndManyConsumer() {
        int count = 100;
        List<CountDownLatch> latches = Stream.generate(() -> new CountDownLatch(count)).limit(count).toList();
        List<Producer> producers = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            consumers.add(new Consumer(i, TOPIC_NAME, broker, latches.get(i)));
            String message = String.valueOf(i);
            producers.add(new Producer(message, TOPIC_NAME, broker));
            messages.add(message);
        }
        List<String> expected = messages.stream().sorted().toList();


        consumers.stream().map(Thread::new).forEach(Thread::start);
        producers.stream().map(Thread::new).forEach(Thread::start);

        latches.forEach(countDownLatch -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });


        consumers.forEach(consumer -> assertEquals(expected, consumer.getMessages().stream().sorted().toList())
        );
    }

}