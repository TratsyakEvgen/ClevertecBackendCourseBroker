package ru.clevertec.broker.client;

import ru.clevertec.broker.Broker;

public class Producer implements Runnable {

    private final String message;
    private final String topicName;
    private final Broker broker;

    public Producer(String message, String topicName, Broker broker) {
        this.message = message;
        this.topicName = topicName;
        this.broker = broker;
    }

    @Override
    public void run() {
        broker.write(topicName, message);
    }
}
