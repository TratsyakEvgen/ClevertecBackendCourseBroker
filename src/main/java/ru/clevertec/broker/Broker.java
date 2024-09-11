package ru.clevertec.broker;

import java.util.concurrent.Semaphore;

public interface Broker {

    void register(String topicName, Semaphore semaphore);

    void write(String topicName, String message);

    String read(String topicName, long consumerId);

}
