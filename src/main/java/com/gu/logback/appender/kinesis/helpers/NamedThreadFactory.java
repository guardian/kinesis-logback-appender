package com.gu.logback.appender.kinesis.helpers;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory that assigns a configurable name to the created threads.
 *
 * Delegates thread creation to the {@link Executors#defaultThreadFactory() default thread factory}.
 */
public class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory delegate = Executors.defaultThreadFactory();
    private final String namePrefix;
    private final AtomicInteger threadCount = new AtomicInteger(1);

    public NamedThreadFactory(String namePrefix) {
        this.namePrefix = Objects.requireNonNull(namePrefix);
    }

    @Override
    public Thread newThread(Runnable r) {
        final Thread thread = delegate.newThread(r);
        thread.setName(namePrefix + threadCount.getAndIncrement());
        return thread;
    }

}
