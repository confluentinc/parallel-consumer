package io.confluent.parallelconsumer;

public class WindowedEventRate {

    private double normalizedRate; // event rate / window
    private long windowSizeTicks;
    private long lastEventTicks;


    public WindowedEventRate(int aWindowSizeSeconds) {
        windowSizeTicks = aWindowSizeSeconds * 1000L;
        lastEventTicks = System.currentTimeMillis();
    }

    public double newEvent() {
//        long currentTicks = System.currentTimeMillis();
        long currentTicks = System.nanoTime();
        long period = currentTicks - lastEventTicks;
        lastEventTicks = currentTicks;
        double normalizedFrequency = (double) windowSizeTicks / (double) period;

        double alpha = Math.min(1.0 / normalizedFrequency, 1.0);
        normalizedRate = (alpha * normalizedFrequency) + ((1.0 - alpha) * normalizedRate);
        return getRate();
    }

    public double getRate() {
        return normalizedRate * 1_000_000L / windowSizeTicks;
    }
}