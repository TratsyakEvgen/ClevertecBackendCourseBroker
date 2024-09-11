package ru.clevertec.broker.exception;

public class TopicException extends RuntimeException {

    public TopicException(String message, Throwable cause) {
        super(message, cause);
    }
}
