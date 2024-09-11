package ru.clevertec.broker.impl;

import ru.clevertec.broker.MessageManager;
import ru.clevertec.broker.exception.TopicException;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Topic implements MessageManager {
    private final Map<Long, Integer> consumerIdMessageIndexMap = new HashMap<>();
    private final List<String> messages = new ArrayList<>();
    private final Lock writeLocker;
    private final Lock readLocker;
    private final Condition condition;
    private final Semaphore semaphore;

    public Topic(Semaphore semaphore) {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.writeLocker = readWriteLock.writeLock();
        this.readLocker = readWriteLock.readLock();
        this.condition = writeLocker.newCondition();
        this.semaphore = semaphore;

    }


    @Override
    public void write(String message) {
        writeLocker.lock();
        try {
            messages.add(message);
            condition.signalAll();
        } finally {
            writeLocker.unlock();
        }

    }

    @Override
    public String read(long consumerId) {
        try {
            semaphore.acquire();
            try {
                int index = getIndexNewMessage(consumerId);
                waitIfNotExistsNewMassages(consumerId, index);
                return getNewMessage(index);
            } finally {
                semaphore.release();
            }
        } catch (InterruptedException e) {
            throw new TopicException("Cannot read message", e);
        }
    }

    private int getIndexNewMessage(long consumerId) {
        readLocker.lock();
        try {
            return Optional.ofNullable(consumerIdMessageIndexMap.get(consumerId)).orElse(0);
        } finally {
            readLocker.unlock();
        }
    }


    private void waitIfNotExistsNewMassages(long consumerId, int position) {
        writeLocker.lock();
        try {
            if (messages.size() <= position) {
                condition.await();
            }
            consumerIdMessageIndexMap.put(consumerId, position + 1);
        } catch (InterruptedException e) {
            throw new TopicException("Cannot lock current thread", e);
        } finally {
            writeLocker.unlock();
        }
    }

    private String getNewMessage(int position) {
        readLocker.lock();
        try {
            return messages.get(position);
        } finally {
            readLocker.unlock();
        }
    }


}
