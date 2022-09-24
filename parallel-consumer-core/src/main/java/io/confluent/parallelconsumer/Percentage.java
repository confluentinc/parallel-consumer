package io.confluent.parallelconsumer;

import lombok.Value;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Value
public class Percentage {

    double value;

    public static Percentage fromDouble(double decimal) {
        assert decimal > 0;
        return new Percentage(decimal);
    }
}
