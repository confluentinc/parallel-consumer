package io.confluent.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.ToDoubleFunction;

import static java.util.Collections.emptyList;

/**
 * Main metrics collection and initialization service.
 * Singleton - makes it easier to add metrics throughout the code
 */
@Slf4j
public class PCMetrics {
    private static PCMetrics instance;

    /**
     * Meter registry used for metrics - set through init call on singleton initialization. Configurable through
     * Parallel Consumer Options.
     */
    private MeterRegistry meterRegistry;

    /**
     * Tracking of registered meters for removal from registry on shutdown.
     */
    private List<Meter.Id> registeredMeters = new ArrayList<>();

    /**
     * Common metrics tags added to all meters - for example PC instance. Configurable through Parallel Consumer
     * Options.
     */
    private Iterable<Tag> commonTags;

    private PCMetrics(MeterRegistry meterRegistry, Iterable<Tag> commonTags) {
        this.meterRegistry = meterRegistry;
        this.commonTags = commonTags;
    }

    /**
     * Singleton initialization - mandatory to be performed before using the singleton through {@link #getInstance()}.
     *
     * @param meterRegistry: meterRegistry to use for meter registration - configured through
     *                       {@link io.confluent.parallelconsumer.ParallelConsumerOptions} on PC initialization
     * @param commonTags:    set of tags to add to all meters - for example - PC instance.
     */
    public static void initialize(MeterRegistry meterRegistry, Iterable<Tag> commonTags) {
        if(instance != null){
            log.warn("Reinitializing PCMetrics without closing them first. Closing previous instance.");
            close();
        }
        instance = new PCMetrics(meterRegistry, commonTags);
    }

    public static PCMetrics getInstance() {
        if (instance == null) {
            log.warn("Warning - trying to use PCMetrics without first initializing it with MeterRegistry. Default noop meter registry will be used.");
            initialize(new CompositeMeterRegistry(), emptyList());
        }
        return instance;
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

    public MeterRegistry getMeterRegistry() {
        return this.meterRegistry;
    }

    /**
     * Resets the singleton instance.
     */
    public static void close() {
        if (instance == null) {
            return;
        }
        log.debug("Resetting PCMetrics");
        // clean up the instance resources
        MeterRegistry registry = instance.getMeterRegistry();
        instance.registeredMeters.forEach(registry::remove);
        instance.registeredMeters.clear();
        // clear instance
        instance = null;
    }

    public static void removeMeter(Meter meter) {
        if (meter != null) {
            removeMeter(meter.getId());
        }
    }

    /**
     * Removes the metric from the singletons meter registry.
     *
     * @param meterId the meter id to remove.
     */
    public static void removeMeter(Meter.Id meterId) {
        log.debug("Removing meter: {}", meterId);
        if (instance != null) {
            instance.getMeterRegistry().remove(meterId);
            instance.registeredMeters.remove(meterId);
        }
    }
}
