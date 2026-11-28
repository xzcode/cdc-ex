package com.cdcex.runtime;

/** Samples durable cumulative counters against a monotonic clock. Not an estimate from queue size or sequence IDs. */
final class RateSampler {
    record Rates(double capturedPerSecond,double deliveredPerSecond,long windowMillis){}
    private long sampleNanos,total,delivered;
    private boolean initialized;
    private Rates rates;
    synchronized Rates sample(long now,long captured,long acknowledgedCount){
        if(!initialized||captured<total||acknowledgedCount<delivered){
            initialized=true;sampleNanos=now;total=captured;delivered=acknowledgedCount;rates=null;return null;
        }
        long elapsed=now-sampleNanos;
        if(elapsed<1_000_000_000L)return rates;
        double seconds=elapsed/1_000_000_000.0;
        rates=new Rates((captured-total)/seconds,(acknowledgedCount-delivered)/seconds,elapsed/1_000_000L);
        sampleNanos=now;total=captured;delivered=acknowledgedCount;
        return rates;
    }
}
