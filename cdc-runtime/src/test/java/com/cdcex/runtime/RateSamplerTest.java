package com.cdcex.runtime;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RateSamplerTest {
    @Test void durableDeltasUseElapsedTimeAndIgnoreShortSamples(){
        RateSampler sampler=new RateSampler();
        assertNull(sampler.sample(0,100,80));
        assertNull(sampler.sample(500_000_000L,110,82));
        var sample=sampler.sample(2_000_000_000L,120,90);
        assertEquals(10.0,sample.capturedPerSecond(),0.0001);
        assertEquals(5.0,sample.deliveredPerSecond(),0.0001);
        assertEquals(2000,sample.windowMillis());
        assertSame(sample,sampler.sample(2_100_000_000L,122,91));
        var idle=sampler.sample(4_000_000_000L,120,90);
        assertEquals(0,idle.capturedPerSecond());assertEquals(0,idle.deliveredPerSecond());
        assertNull(sampler.sample(5_000_000_000L,1,0));
    }
}
