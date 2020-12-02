package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BackoffAnalyser {

    @Getter
    private int currentTotalMaxCountBeyondOffset = 10_000;

    private int stepUp = 10_000;

    private double backoffMultiplier = 0.5; // aggressive

    private double failureApprachThreshold = 0.2; // 20%

    private List<Integer> failureAt = new ArrayList<>();

    public BackoffAnalyser(final int initialMax) {
        this.currentTotalMaxCountBeyondOffset = initialMax;
    }

    void onSuccess() {
        if (!failureAt.isEmpty()) {
            int recentFail = failureAt.get(failureAt.size() - 1);
            int limit = (int) (recentFail * (1 - failureApprachThreshold));
            if (currentTotalMaxCountBeyondOffset + stepUp < limit) {
                currentTotalMaxCountBeyondOffset += stepUp;
            }
        } else {
            currentTotalMaxCountBeyondOffset += stepUp;
        }
        log.debug("Succeeded, stepping up by {} to {}", stepUp, currentTotalMaxCountBeyondOffset);
    }

    void onFailure() {
        failureAt.add(currentTotalMaxCountBeyondOffset);
        currentTotalMaxCountBeyondOffset = (int) (currentTotalMaxCountBeyondOffset * backoffMultiplier);
        log.debug("Failed, backing off by {} to {}", backoffMultiplier, currentTotalMaxCountBeyondOffset);
    }

}
