package ru.clevertec.broker.impl;

import ru.clevertec.broker.Broker;
import ru.clevertec.broker.MessageManager;
import ru.clevertec.broker.exception.BrokerException;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class TopicBroker implements Broker {

    private final Map<String, MessageManager> nameTopicMap = new ConcurrentHashMap<>();


    @Override
    public void register(String topicName, Semaphore semaphore) {
        if (nameTopicMap.containsKey(topicName)) {
            throw new BrokerException(
                    String.format("Topic with name %s already exists", topicName)
            );
        }
        nameTopicMap.put(topicName, new Topic(semaphore));

    }

    @Override
    public void write(String topicName, String message) {
        MessageManager messageManager = getMessageManger(topicName);
        messageManager.write(message);
    }

    @Override
    public String read(String topicName, long consumerId) {
        MessageManager messageManager = getMessageManger(topicName);
        return messageManager.read(consumerId);
    }

    private MessageManager getMessageManger(String topicName) {
        return Optional.ofNullable(nameTopicMap.get(topicName))
                .orElseThrow(() -> new BrokerException("Not found topic with name " + topicName));
    }

}

