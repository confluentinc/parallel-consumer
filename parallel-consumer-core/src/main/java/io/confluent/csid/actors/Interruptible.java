package io.confluent.csid.actors;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface Interruptible {
    void interruptProcessBlockingMaybe(Reason reason);

    /**
     * todo docs
     */
    @AllArgsConstructor
    @RequiredArgsConstructor
    @ToString
    @EqualsAndHashCode
    class Reason {
        public static final Reason UNKNOWN = new Reason("unknown-reason");
        private final String desc;
        private Reason root;
    }
}
