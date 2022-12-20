package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Loop implementations that will resume from a given key. Can be constructed and used as an iterable, or a function
 * passed into the static version {@link #iterateStartingFromKeyLooping}.
 * <p>
 * Uses a looser contract than {@link Iterator} - that being it has no #hasNext() method - instead, it's {@link #next()}
 * returns {@link Optional#empty()} when it's done.
 * <p>
 * The non-functional version is useful when you want to use looping constructs such as {@code break} and
 * {@code continue}.
 * <p>
 *
 * @author Antony Stubbs
 */
@Slf4j
public class LoopingResumingIterator<KEY, VALUE> {

    private Optional<Map.Entry<KEY, VALUE>> head = Optional.empty();

    /**
     * See {@link java.util.concurrent.ConcurrentHashMap} docs on iteration
     *
     * @see java.util.concurrent.ConcurrentHashMap.Traverser
     */
    private Iterator<Map.Entry<KEY, VALUE>> wrappedIterator;

    /**
     * As {@link java.util.concurrent.ConcurrentHashMap}'s iterators are thread safe, they see a snapshot of the map in
     * time - this may cause the starting point key to be removed. In which case, we limit our iteration to taking the
     * expected number of elements.
     * <p>
     */
    private final long iterationTargetCount;

    /**
     * The number of iterations we've done so far.
     *
     * @see #iterationTargetCount
     */
    private long iterationCount = 0;

    /**
     * The key to start from
     */
    @Getter
    private final Optional<KEY> iterationStartingPointKey;

    private final Map<KEY, VALUE> map;

    /**
     * Where the iteration of the collection has now started again from index zero.
     * <p>
     * Binary, as can only loop once after reach the end (to reach the initial starting point again).
     */
    private boolean isOnSecondPass = false;

    /**
     * Iteration has fully completed, and the collection is now exhausted.
     */
    private boolean terminalState = false;

    /**
     * A start key was provided, and it was found in the collection.
     */
    private boolean startingPointKeyValid = false;

    public static <KKEY, VVALUE> LoopingResumingIterator<KKEY, VVALUE> build(KKEY startingKey, Map<KKEY, VVALUE> map) {
        return new LoopingResumingIterator<>(Optional.ofNullable(startingKey), map);
    }

    /**
     * Will resume from the startingKey, if it's present
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public LoopingResumingIterator(Optional<KEY> startingKey, Map<KEY, VALUE> map) {
        this.iterationStartingPointKey = startingKey;
        this.map = map;
        this.wrappedIterator = map.entrySet().iterator();
        this.iterationTargetCount = map.size();

        // find the starting point
        if (startingKey.isPresent()) {
            this.head = advanceToStartingPointAndGet(startingKey.get());
            if (head.isPresent()) {
                this.startingPointKeyValid = true;
            } else {
                resetIteratorToZero();
            }
        }
    }

    public LoopingResumingIterator(Map<KEY, VALUE> map) {
        this(Optional.empty(), map);
    }


    /**
     * @return null if no more elements
     */
    public Optional<Map.Entry<KEY, VALUE>> next() {
        iterationCount++;

        // special cases
        if (terminalState) {
            return Optional.empty();
        } else if (this.head.isPresent()) {
            Optional<Map.Entry<KEY, VALUE>> headSave = takeHeadValue();
            return headSave;
        }

        if (wrappedIterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = wrappedIterator.next();
            // could find the starting point earlier
            boolean onSecondPassAndReachedStartingPoint = iterationStartingPointKey.equals(Optional.of(next.getKey()));
            // or it could be missing entirely
            boolean numberElementsReturnedExceeded = iterationCount > iterationTargetCount + 1; // off by one due to eager increment
            if (onSecondPassAndReachedStartingPoint || numberElementsReturnedExceeded) {
                // end second iteration reached
                terminalState = true;
                return Optional.empty();
            } else {
                return Optional.ofNullable(next);
            }
        } else if (iterationStartingPointKey.isPresent() && startingPointKeyValid && !isOnSecondPass) {
            // we've reached the end, but we have a starting point set, so loop back to the start and do second pass
            resetIteratorToZero();
            isOnSecondPass = true;
            return next();
        } else {
            // end of 2nd pass
            return Optional.empty();
        }
    }

    private Optional<Map.Entry<KEY, VALUE>> takeHeadValue() {
        var headSave = head;
        head = Optional.empty();
        return headSave;
    }

    /**
     * Finds the starting point entry, and sets its index if found.
     *
     * @return the starting point entry, if found. Otherwise, null.
     * @see #startingPointIndex
     */
    private Optional<Map.Entry<KEY, VALUE>> advanceToStartingPointAndGet(Object startingPointObject) {
        while (wrappedIterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = wrappedIterator.next();
            if (next.getKey() == startingPointObject) {
                return Optional.of(next);
            }
        }
        return Optional.empty();
    }

    private void resetIteratorToZero() {
        wrappedIterator = map.entrySet().iterator();
    }

}
