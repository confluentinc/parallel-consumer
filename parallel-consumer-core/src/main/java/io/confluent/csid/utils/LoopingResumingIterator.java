package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static io.confluent.csid.utils.BackportUtils.hasNo;

/**
 * Loop implementations that will resume from a given key. Can be constructed and used as an iterable, or a function
 * passed into the static version {@link #iterateStartingFromKeyLooping}.
 * <p>
 * The non-functional version is useful when you want to use looping constructs such as {@code break} and
 * {@code continue}.
 * <p>
 * TODO surely there's a better (standard library) way to do this - try to remove this class - too complex
 *
 * @author Antony Stubbs
 */
@Slf4j
public class LoopingResumingIterator<KEY, VALUE> implements Iterator<Map.Entry<KEY, VALUE>>, Iterable<Map.Entry<KEY, VALUE>> {

    private Optional<Map.Entry<KEY, VALUE>> head = Optional.empty();

    /**
     * See {@link java.util.concurrent.ConcurrentHashMap} docs on iteration
     *
     * @see java.util.concurrent.ConcurrentHashMap.Traverser
     */
    private Iterator<Map.Entry<KEY, VALUE>> iterator;

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
     * Tracks starting point with an index, instead of just object equality, in case the collection contains to elements
     * that are equal.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<Integer> startingPointIndex = Optional.empty();

    /**
     * Tracks the index of the element, which the wrapped iterator will retrieve next
     */
    private int indexOfNextElementIteratorWillRetrieve = 0;

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
        iterator = map.entrySet().iterator();

        // find the starting point
        if (startingKey.isPresent()) {
            this.head = advanceToStartingPointAndGet(startingKey.get());
            if (head.isEmpty()) {
                startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
                resetIteratorToZero();
            }
        } else {
            startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
        }

    }

    public LoopingResumingIterator(Map<KEY, VALUE> map) {
        this(Optional.empty(), map);
    }

    @Override
    public boolean hasNext() {
        if (hasNo(iterationStartingPointKey)) {
            // reverts to simple non-resuming case
            return iterator.hasNext();
        } else if (isOnSecondPass) {
            boolean endOfSecondIterationReached = startingPointIndex.orElse(-1) == indexOfNextElementIteratorWillRetrieve;
            // if has not ended 2nd iteration, elements remain
            return !endOfSecondIterationReached;
        } else {
            return firstIterationHasNext();
        }
    }

    private boolean firstIterationHasNext() {
        if (iterator.hasNext()) {
            // return the underlying iterators value
            return true;
        } else {
            // end of first iteration reached
            boolean hasPositiveStartingPoint = isStartingPointIndexFound() && startingPointIndex.get() != 0;
            if (hasPositiveStartingPoint) {
                isOnSecondPass = true;
                // reset the iterator
                resetIteratorToZero();
                return iterator.hasNext();
            } else {
                // from not found or was first element, there won't be a second pass
                return false;
            }
        }
    }

    @Override
    public Map.Entry<KEY, VALUE> next() { // NOSONAR - NoSuchElementException thrown in nested method
        if (this.head.isPresent()) {
            var headValue = head.get();
            head = Optional.empty();
            return headValue;
        } else if (iterationStartingPointKey.isPresent()) {
            return findStartingPointAndNextValue(iterationStartingPointKey.get());
        } else {
            return advanceIndexAndGetNext();
        }
    }

    private Map.Entry<KEY, VALUE> findStartingPointAndNextValue(Object startingPointObject) {
        if (isStartingPointIndexFound()) {
            return getNextAndMaybeLoop(startingPointObject);
        } else {
            return setStartAndGet(startingPointObject);
        }
    }

    private boolean isStartingPointIndexFound() {
        return startingPointIndex.isPresent();
    }

    /**
     * Tries to find the starting point, and returns the corresponding object if found.
     */
    private Map.Entry<KEY, VALUE> setStartAndGet(Object startingPointObject) {
        Optional<Map.Entry<KEY, VALUE>> startingPoint = advanceToStartingPointAndGet(startingPointObject);
        boolean startingPointFound = startingPoint.isPresent();
        if (startingPointFound) {
            return startingPoint.get();
        } else {
            startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
            resetIteratorToZero();
            return advanceIndexAndGetNext(); // return first element
        }
    }

    /**
     * Finds the starting point entry, and sets its index if found.
     *
     * @return the starting point entry, if found. Otherwise, null.
     * @see #startingPointIndex
     */
    private Optional<Map.Entry<KEY, VALUE>> advanceToStartingPointAndGet(Object startingPointObject) {
        while (iterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = advanceIndexAndGetNext();
            if (next.getKey() == startingPointObject) {
                int value = indexOfNextElementIteratorWillRetrieve - 1;
                if (value < 0) {
                    value = this.map.size() - 1;
                }
                startingPointIndex = Optional.of(value);
                return Optional.of(next);
            }
        }
        return Optional.empty();
    }

    private Map.Entry<KEY, VALUE> getNextAndMaybeLoop(Object startingPointObject) {
        Map.Entry<KEY, VALUE> toReturn;
        if (isOnSecondPass) {
            toReturn = advanceIndexAndGetNext(); // ConcurrentHashMap.Traverser never throws NoSuchElementException
            if (toReturn.getKey() == startingPointObject) {
                // back at the beginning
                throw new NoSuchElementException("#hasNext() returned true, but there are no more entries that haven't been iterated.");
            }
        } else {
            // still on first pass
            if (iterator.hasNext()) {
                toReturn = advanceIndexAndGetNext();
            } else {
                // reached end of first pass
                isOnSecondPass = true;
                resetIteratorToZero();
                // return first looped value
                toReturn = advanceIndexAndGetNext();
            }
        }
        return toReturn;
    }

    private void resetIteratorToZero() {
        indexOfNextElementIteratorWillRetrieve = 0;
        iterator = map.entrySet().iterator();
    }

    private Map.Entry<KEY, VALUE> advanceIndexAndGetNext() {
        final boolean indexAtEndOfMap = indexOfNextElementIteratorWillRetrieve == this.map.size() - 1;
        if (indexAtEndOfMap) {
            // reset to beginning
            indexOfNextElementIteratorWillRetrieve = 0;
        } else {
            indexOfNextElementIteratorWillRetrieve++;
        }
        return iterator.next();
    }

    @Override
    public Iterator<Map.Entry<KEY, VALUE>> iterator() {
        return this; // NOSONAR - this represents the iteration over the underlying collection as well
    }

}
