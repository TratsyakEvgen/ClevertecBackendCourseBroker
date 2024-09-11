package ru.clevertec.broker.client;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.clevertec.broker.Broker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@RequiredArgsConstructor
public class Consumer implements Runnable {
    private final long id;
    private final String topicName;
    private final Broker broker;
    private final CountDownLatch countDownLatch;
    @Getter
    List<String> messages = new ArrayList<>();


    @Override
    public void run() {
        while (countDownLatch.getCount() > 0) {
            String message = broker.read(topicName, id);
            messages.add(message);
            countDownLatch.countDown();
        }
    }
}
