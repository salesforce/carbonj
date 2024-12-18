package com.demandware.carbonj.service.engine.destination;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.demandware.carbonj.service.engine.DataPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestNullDestination {
    @Test
    public void test() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        NullDestination nullDestination = new NullDestination(metricRegistry, "audit", "TestNullDestination");
        DataPoint dataPoint = new DataPoint("foo.bar", 123, (int) (System.currentTimeMillis() / 1000));
        nullDestination.accept(dataPoint);
        checkMeter(metricRegistry, nullDestination.name + ".recv", 1L);
        checkMeter(metricRegistry, nullDestination.name + ".sent", 1L);
        nullDestination.closeQuietly();
    }

    private void checkMeter(MetricRegistry metricRegistry, String name, long expected) {
        Meter meter = metricRegistry.getMeters().get(name);
        assertNotNull(meter);
        assertEquals(expected, meter.getCount());
    }
}
