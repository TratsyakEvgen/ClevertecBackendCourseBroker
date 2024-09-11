package ru.clevertec.broker;

public interface MessageManager {
    void write(String message);

    String read(long consumerId);
}
