package com.distributedsystems.paxos.util;

import java.util.concurrent.atomic.AtomicLong;

public final class BallotGenerator {
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private BallotGenerator() {}
    public static long nextBallot(int nodeId) {
        return COUNTER.incrementAndGet();
    }
}
