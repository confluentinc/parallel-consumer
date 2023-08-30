package io.confluent.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.search.Search;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToDoubleFunction;

import static java.util.Collections.singleton;

/**
 * Main metrics collection and initialization service. Singleton - makes it easier to add metrics throughout the code
 */
@Slf4j
public class PCMetrics {

    /**
     * Meter registry used for metrics - set through init call on singleton initialization. Configurable through
     * Parallel Consumer Options.
     */
    @Getter
    private MeterRegistry meterRegistry;

    /**
     * Tracking of registered meters for removal from registry on shutdown.
     */
    private List<Meter.Id> registeredMeters = new ArrayList<>();

    /**
     * Common metrics tags added to all meters - for example PC instance. Configurable through Parallel Consumer
     * Options.
     */
    @Getter
    private Iterable<Tag> commonTags;

    @Getter
    private Tag instanceTag;

    private final AtomicBoolean isClosed = new AtomicBoolean(true);

    private final boolean isNoop;

    /**
     * @param meterRegistry: meterRegistry to use for meter registration - configured through
     *                       {@link io.confluent.parallelconsumer.ParallelConsumerOptions} on PC initialization
     * @param commonTags:    set of tags to add to all meters - for example - PC instance.
     */
    public PCMetrics(MeterRegistry meterRegistry, Iterable<Tag> commonTags, String instanceTag) {
        if (meterRegistry == null) {
            this.isNoop = true;
            this.meterRegistry = new CompositeMeterRegistry();
        } else {
            this.isNoop = false;
            this.meterRegistry = meterRegistry;
        }
        if (instanceTag != null) {
            this.instanceTag = Tag.of(PCMetricsDef.PC_INSTANCE_TAG, instanceTag);
        } else {
            this.instanceTag = generateUniqueInstanceTag();
        }
        this.commonTags = combine(this.instanceTag, commonTags);
        this.isClosed.set(false);
    }

    /**
     * Combines instance tag and common tags specified while ensuring there are no tags with same tag key.
     *
     * @param instanceTag
     * @param commonTags
     * @return combined tag collection with unique tag keys
     */
    private Iterable<Tag> combine(Tag instanceTag, Iterable<Tag> commonTags) {
        Set<String> tagKeys = new HashSet<>();
        List<Tag> tags = new LinkedList<>();

        tagKeys.add(instanceTag.getKey());
        tags.add(instanceTag);
        commonTags.forEach(tag -> {
            if (!tagKeys.contains(tag.getKey())) {
                tagKeys.add(tag.getKey());
                tags.add(tag);
            } else {
                log.warn("Duplicate metrics tag specified : {}", tag.getKey());
            }
        });
        return tags;
    }


    private Tag generateUniqueInstanceTag() {
        boolean inUse;
        Tag tagToUse;
        do {
            tagToUse = Tag.of(PCMetricsDef.PC_INSTANCE_TAG, UUID.randomUUID().toString());
            inUse = Search.in(meterRegistry).tags(singleton(instanceTag)).meter() != null;
        } while (inUse);
        return tagToUse;
    }

    /**
     * Returns a counter from the metric definition. The counter will be registered with the meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the counter.
     */
    public Counter getCounterFromMetricDef(PCMetricsDef metricDef, Tag... additionalTags) {
        Counter counter = Counter.builder(metricDef.getName())
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(counter.getId());
        return counter;
    }

    /**
     * Returns a timer from the metric definition. The timer will be registered with the meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the timer.
     */
    public Timer getTimerFromMetricDef(PCMetricsDef metricDef, Tag... additionalTags) {
        Timer timer = Timer.builder(metricDef.getName())
                .publishPercentiles(0, 0.5, 0.75, 0.95, 0.99, 0.999)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(timer.getId());
        return timer;
    }

    /**
     * Returns a gauge from the metric definition. The gauge will be registered with the meter. The returned Gauge
     * instance is not useful except in testing, as the gauge is already set up to track a value automatically upon
     * registration.
     *
     * <p><strong>Note: Make sure you hold a strong reference to your object. Otherwise once the
     * object being gauged is re-referenced and is garbage collected, micrometer starts reporting NaN or nothing for a
     * gauge</strong>
     *
     * <p>See <a
     * href="https://github.com/micrometer-metrics/micrometer-docs/blob/main/src/docs/concepts/gauges.adoc#why-is-my-gauge-reporting-nan-or-disappearing">micrometer
     * docs</a> for more info
     *
     * @param metricDef:      the metric definition to use.
     * @param stateObject:    object to collect metrics from
     * @param valueFunction:  function of the stateObject that is invoked on gauge observation to return the value
     * @param additionalTags: additional tags to add to the gauge.
     * @return the Gauge instance.
     */
    public <T> Gauge gaugeFromMetricDef(
            PCMetricsDef metricDef,
            T stateObject,
            ToDoubleFunction<T> valueFunction,
            Tag... additionalTags) {
        Gauge gauge = Gauge.builder(metricDef.getName(), stateObject, valueFunction)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .strongReference(true)
                .register(this.meterRegistry);
        registeredMeters.add(gauge.getId());
        return gauge;
    }

    /**
     * Returns a distribution summary from the metric definition. The distribution summary will be registered with the
     * meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the distribution summary.
     * @return the DistributionSummary instance.
     */
    public DistributionSummary getDistributionSummaryFromMetricDef(
            PCMetricsDef metricDef, Tag... additionalTags) {
        DistributionSummary distributionSummary = DistributionSummary.builder(metricDef.getName())
                .publishPercentiles(0, 0.5, 0.75, 0.95, 0.99, 0.999)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(distributionSummary.getId());
        return distributionSummary;
    }

    /**
     * Closes PCMetrics object and cleans up all meters from registry - should be recreated before using it again.
     */
    public synchronized void close() {
        if (this.isClosed.getAndSet(true)) {
            //Instance already closed - warn and ignore.
            log.warn("Trying to close PCMetrics instance that is already closed.");
            return;
        }
        log.debug("Closing PCMetrics");
        // clean up the instance resources
        this.registeredMeters.forEach(this.meterRegistry::remove);
        this.registeredMeters.clear();
        if (isNoop) {
            this.meterRegistry.close();
        }
    }

    /**
     * Removes the metric from the singletons meter registry.
     * <p>
     * Synchronized with close method to avoid concurrent modification race on shutdown between removal of partition
     * meters on revocation and closing metrics subsystem
     *
     * @param meter to remove.
     */
    public synchronized void removeMeter(Meter meter) {
        if (meter != null) {
            removeMeter(meter.getId());
        }
    }


    private void removeMeter(Meter.Id meterId) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meter when metrics subsystem is already closed. Meter Id {}", meterId);
            return;
        }
        log.debug("Removing meter: {}", meterId);
        this.meterRegistry.remove(meterId);
        this.registeredMeters.remove(meterId);
    }

    public void removeMetersByPrefixAndCommonTags(String meterNamePrefix) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meters when metrics subsystem is already closed.");
            return;
        }
        Search.in(meterRegistry).name(name -> name.startsWith(meterNamePrefix))
                .tags(commonTags).meters().forEach(meterRegistry::remove);
    }
}