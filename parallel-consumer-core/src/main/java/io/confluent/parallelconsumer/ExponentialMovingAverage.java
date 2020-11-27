package io.confluent.parallelconsumer;

public class ExponentialMovingAverage {

    private double alpha;
    private Double oldValue;

    public ExponentialMovingAverage(double alpha) {
        this.alpha = alpha;
    }

    public double average(double value) {
        if (oldValue == null) {
            oldValue = value;
            return value;
        }
        double newValue = oldValue + alpha * (value - oldValue);
        oldValue = newValue;
        return newValue;
    }
}
