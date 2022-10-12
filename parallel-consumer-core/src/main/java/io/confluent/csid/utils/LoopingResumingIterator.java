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

import static io.confluent.csid.utils.BackportUtils.isEmpty;

/**
 * Loop implementations that will resume from a given key. Can be constructed and used as an iterable, or a function
 * passed into the static version {@link #iterateStartingFromKeyLooping}.
 * <p>
 * The non functional version is useful when you want to use looping constructs such as {@code break} and {@code
 * continue}.
 */
@Slf4j
public class LoopingResumingIterator<KEY, VALUE> implements Iterator<Map.Entry<KEY, VALUE>>, Iterable<Map.Entry<KEY, VALUE>> {

    private Iterator<Map.Entry<KEY, VALUE>> iterator;

    @Getter
    private final Optional<KEY> iterationStartingPoint;

    private final Map<KEY, VALUE> map;

    private boolean hasLoopedAroundBackToBeginning = false;

    /**
     * Tracks starting point with an index, instead of just object equality, in case the collection contains to elements
     * that are equal.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<Integer> startingPointIndex = Optional.empty();

    private int indexOfNextElementToRetrieve = 0;

    public static <KKEY, VVALUE> LoopingResumingIterator<KKEY, VVALUE> build(KKEY startingKey, Map<KKEY, VVALUE> map) {
        return new LoopingResumingIterator<>(Optional.ofNullable(startingKey), map);
    }

    /**
     * Will resume from the startingKey, if it's present
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public LoopingResumingIterator(Optional<KEY> startingKey, Map<KEY, VALUE> map) {
        this.iterationStartingPoint = startingKey;
        this.map = map;
        var entries = map.entrySet();
        this.iterator = entries.iterator();
    }

    public LoopingResumingIterator(Map<KEY, VALUE> map) {
        this(Optional.empty(), map);
    }

    @Override
    public boolean hasNext() {
        if (isEmpty(iterationStartingPoint)) {
            return iterator.hasNext();
        } else if (hasLoopedAroundBackToBeginning) {
            // we've looped around
            boolean endOfIterationReached = startingPointIndex.orElse(-1) == indexOfNextElementToRetrieve;
            return !endOfIterationReached;
        } else {
            boolean atEndOfFirstIteration = !iterator.hasNext();
            if (atEndOfFirstIteration) {
                boolean startingPointIsntZero = isStartingPointIndexFound() && startingPointIndex.get() != 0;
                if (startingPointIsntZero) {
                    hasLoopedAroundBackToBeginning = true;
                    // reset the iterator
                    resetIterator();
                    return iterator.hasNext();
                } else {
                    // from not found or was first element, there won't be a second pass
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    @Override
    public Map.Entry<KEY, VALUE> next() { // NOSONAR - NoSuchElementException thrown in nested method
        if (iterationStartingPoint.isPresent()) {
            return findStartingPointAndNextValue(iterationStartingPoint.get());
        } else {
            return getNext();
        }
    }

    private Map.Entry<KEY, VALUE> findStartingPointAndNextValue(Object startingPointObject) {
        if (isStartingPointIndexFound()) {
            return getNextAndMaybeLoop(startingPointObject);
        } else {
            return attemptToFindStart(startingPointObject);
        }
    }

    private boolean isStartingPointIndexFound() {
        return startingPointIndex.isPresent();
    }

    /**
     * Tries to find the starting point, and returns the corresponding object if found.
     */
    private Map.Entry<KEY, VALUE> attemptToFindStart(Object startingPointObject) {
        Optional<Map.Entry<KEY, VALUE>> startingPoint = findStartingPointMaybe(startingPointObject);
        if (startingPoint.isPresent()) {
            return startingPoint.get();
        } else {
            startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
            resetIterator();
            return getNext();
        }
    }

    /**
     * @return the starting point entry, if found. Otherwise, null.
     */
    private Optional<Map.Entry<KEY, VALUE>> findStartingPointMaybe(Object startingPointObject) {
        while (iterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = getNext();
            if (next.getKey() == startingPointObject) {
                int value = indexOfNextElementToRetrieve - 1;
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
        if (hasLoopedAroundBackToBeginning) {
            toReturn = getNext();

            if (null != toReturn && toReturn.getKey() == startingPointObject) {
                // back at the beginning
                throw new NoSuchElementException("#hasNext() returned true, but there are no more entries that haven't been iterated.");
            }
        } else {
            // still on first pass
            if (iterator.hasNext()) {
                toReturn = getNext();
            } else {
                // reached end of first pass
                hasLoopedAroundBackToBeginning = true;
                resetIterator();
                // return first looped value
                toReturn = getNext();
            }
        }
        return toReturn;
    }

    private void resetIterator() {
        this.indexOfNextElementToRetrieve = 0;
        iterator = map.entrySet().iterator();
    }

    private Map.Entry<KEY, VALUE> getNext() {
        Map.Entry<KEY, VALUE> toReturn = null;
        if (indexOfNextElementToRetrieve == this.map.size() - 1) {
            indexOfNextElementToRetrieve = 0;
        } else {
            indexOfNextElementToRetrieve++;
        }
        if(iterator.hasNext()){
            toReturn = iterator.next();
        }
        return toReturn;
    }

    @Override
    public Iterator<Map.Entry<KEY, VALUE>> iterator() {
        return this;
    }

}
