package io.confluent.parallelconsumer.truth;

import com.google.common.truth.FailureMetadata;
import io.stubbs.truth.generator.SubjectFactoryMethod;
import io.stubbs.truth.generator.UserManagedMiddleSubject;
import io.stubbs.truth.generator.UserManagedSubject;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarChildSubject;
import me.tongfei.progressbar.ProgressBarParentSubject;

import javax.annotation.Generated;

/**
 * @author Antony Stubbs
 * @see ProgressBar
 * @see ProgressBarParentSubject
 * @see ProgressBarChildSubject
 */
@UserManagedSubject(ProgressBar.class)
@Generated(value = "io.stubbs.truth.generator.internal.TruthGenerator", date = "2022-12-22T22:28:20.052461Z")
public class ProgressBarSubject extends ProgressBarParentSubject implements UserManagedMiddleSubject {

    protected ProgressBarSubject(FailureMetadata failureMetadata, ProgressBar actual) {
        super(failureMetadata, actual);
    }

    /**
     * Returns an assertion builder for a {@link ProgressBar} class.
     */
    @SubjectFactoryMethod
    public static Factory<ProgressBarSubject, ProgressBar> progressBars() {
        return ProgressBarSubject::new;
    }

    public void isFinalRateAtLeast(int atLeastRate) {
        var current = actual.getCurrent();
        var totalElapsed = actual.getTotalElapsed();
        var finalRate = current / totalElapsed.getSeconds();

        check("finalRate").that(finalRate).isAtLeast(atLeastRate);
    }
}
