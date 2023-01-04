package io.raft.core;

import jdk.incubator.concurrent.StructuredTaskScope;

import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class FirstNTasksScope<T> extends StructuredTaskScope<T> {
    private final int majority;
    private final AtomicInteger finishedTasks = new AtomicInteger();
    private final BlockingQueue<T> results = new LinkedBlockingQueue<>();
    private final BlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();

    public FirstNTasksScope(int majority) {
        super("n-task-scope", Thread.ofVirtual().factory());
        this.majority = majority;
    }

    @Override
    protected void handleComplete(Future<T> future) {
        switch (future.state()) {
            case RUNNING -> throw new IllegalArgumentException("Task is not completed");
            case SUCCESS -> {
                T result = future.resultNow();
                results.offer(result);
                int finished = finishedTasks.incrementAndGet();
                if (finished >= majority) {
                    shutdown();
                }
            }
            case FAILED -> {
                exceptions.offer(future.exceptionNow());
            }
            case CANCELLED -> {
            }
        }
    }

    @Override
    public void joinUntil(Instant deadline) throws InterruptedException {
        try {
            super.joinUntil(deadline);
        } catch (TimeoutException e) {
        }
    }

    public Queue<T> results() {
        return results;
    }
}
