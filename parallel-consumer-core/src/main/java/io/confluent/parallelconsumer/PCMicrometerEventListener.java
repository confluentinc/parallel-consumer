package io.confluent.parallelconsumer;

import com.google.common.eventbus.Subscribe;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@NoArgsConstructor
public class PCMicrometerEventListener implements MeterBinder {
    MeterRegistry meterRegistry;
    ConcurrentHashMap<Meter.Id, Double> gaugesTracker = new ConcurrentHashMap<>();

    @Subscribe
    public void eventHandler(final MetricsEvent event){
        switch (event.getType()){
            case COUNTER -> Optional.ofNullable(
                    meterRegistry.find(event.getName()).tags(event.getTags()).counter()).orElseGet(()->
                    bindAndGetCounter(event.getName(), event.getDescription(), event.getTags())).increment();
            case GAUGE -> gaugesTracker.put(Optional.ofNullable(
                    meterRegistry.find(event.getName()).tags(event.getTags()).meter())
                            .map(Meter::getId).orElseGet(()->
                    bindAndGetGauge(event.getName(), event.getDescription(), event.getTags()).getId()), event.getValue());
            case TIMER -> Optional.ofNullable(
                    meterRegistry.find(event.getName()).tags(event.getTags()).timer()).orElseGet(()->
                    bindAndGetTimer(event.getName(), event.getDescription(), event.getTags()))
                    .record(event.getTimerValue());
        }
    }

    private Counter bindAndGetCounter(String name, String desc, Iterable<Tag> tags) {
        return Counter.builder(name)
                .description(desc)
                .tags(tags)
                .register(this.meterRegistry);
    }

    private Timer bindAndGetTimer(String name, String desc, Iterable<Tag> tags) {
        return Timer.builder(name)
                .description(desc)
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5,0.95,0.99,0.999)
                .tags(tags)
                .register(this.meterRegistry);
    }

    private Gauge bindAndGetGauge(String name, String desc, Iterable<Tag> tags) {
        var id = new Meter.Id(name, Tags.of(tags), null, desc, Meter.Type.GAUGE);
        var gauge = Gauge.builder(name, gaugesTracker,
                tracker -> tracker.getOrDefault(id, 0.0))
                .description(desc)
                .tags(tags)
                .register(this.meterRegistry);
        gaugesTracker.put(id, 0.0);
        return gauge;
    }

    @Override
    public void bindTo(@NonNull final MeterRegistry meterRegistry) {
        this.meterRegistry=meterRegistry;
    }
}
