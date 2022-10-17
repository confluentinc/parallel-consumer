package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.Getter;
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
public class LoopingResumingIterator<KEY, VALUE>
//        implements Iterable<Map.Entry<KEY, VALUE>>
{

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
     * Iteration has fully completed, and the collection is now exhausted.
     */
    private boolean terminalState;

    /**
     * A start key was provided, and it was found in the collection.
     */
    private boolean startingPointKeyValid;

    /**
     * Tracks starting point with an index, instead of just object equality, in case the collection contains to elements
     * that are equal.
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
//    private Optional<Integer> startingPointIndex = Optional.empty();

    /**
     * Tracks the index of the element, which the wrapped iterator will retrieve next.
     * <p>
     * Need to use indexes because we need to implement we cannot "peek" at what the #next() method will return, we must
     * call it - this of course moves the pointer.
     */
//    private int indexOfNextElementIteratorWillRetrieve = 0;

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
//                startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
                resetIteratorToZero();
            } else {
                startingPointKeyValid = true;
            }
        }
//        else {
//            startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
//        }

    }

    public LoopingResumingIterator(Map<KEY, VALUE> map) {
        this(Optional.empty(), map);
    }


    /**
     * @return null if no more elements
     */
    //    @Override
    public Optional<Map.Entry<KEY, VALUE>> next() {
        // special cases
        if (terminalState) {
            return Optional.empty();
        } else if (this.head.isPresent()) {
            Optional<Map.Entry<KEY, VALUE>> headSave = takeHeadValue();
            return headSave;
        }

        if (iterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = iterator.next();
            boolean onSecondPassAndReachedStartingPoint = iterationStartingPointKey.equals(Optional.of(next.getKey()));
            if (onSecondPassAndReachedStartingPoint) {
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

//    private Map.Entry<KEY, VALUE> findStartingPointAndNextValue(Object startingPointObject) {
////        if (isStartingPointIndexFound()) {
//            return getNextAndMaybeLoop(startingPointObject);
////        } else {
////            return setStartAndGet(startingPointObject);
////        }
//    }

//    private boolean isStartingPointIndexFound() {
//        return startingPointIndex.isPresent();
//    }
//
//    /**
//     * Tries to find the starting point, and returns the corresponding object if found.
//     */
//    private Map.Entry<KEY, VALUE> setStartAndGet(Object startingPointObject) {
//        Optional<Map.Entry<KEY, VALUE>> startingPoint = advanceToStartingPointAndGet(startingPointObject);
//        boolean startingPointFound = startingPoint.isPresent();
//        if (startingPointFound) {
//            return startingPoint.get();
//        } else {
//            startingPointIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
//            resetIteratorToZero();
////            return advanceIndexAndGetNext(); // return first element
//        }
//    }

    /**
     * Finds the starting point entry, and sets its index if found.
     *
     * @return the starting point entry, if found. Otherwise, null.
     * @see #startingPointIndex
     */
    private Optional<Map.Entry<KEY, VALUE>> advanceToStartingPointAndGet(Object startingPointObject) {
        while (iterator.hasNext()) {
            Map.Entry<KEY, VALUE> next = iterator.next();
            if (next.getKey() == startingPointObject) {
//                int value = indexOfNextElementIteratorWillRetrieve - 1;
//                if (value < 0) {
//                    value = this.map.size() - 1;
//                }
//                startingPointIndex = Optional.of(value);
                return Optional.of(next);
            }
        }
        return Optional.empty();
    }

//    private Map.Entry<KEY, VALUE> getNextAndMaybeLoop(Object startingPointObject) {
//        Map.Entry<KEY, VALUE> toReturn;
//        if (isOnSecondPass) {
//            toReturn = advanceIndexAndGetNext(); // ConcurrentHashMap.Traverser never throws NoSuchElementException
//            if (toReturn.getKey() == startingPointObject) {
//                // back at the beginning
//                throw new NoSuchElementException("#hasNext() returned true, but there are no more entries that haven't been iterated.");
//            }
//        } else {
//            // still on first pass
//            if (iterator.hasNext()) {
//                toReturn = advanceIndexAndGetNext();
//            } else {
//                // reached end of first pass
//                isOnSecondPass = true;
//                resetIteratorToZero();
//                // return first looped value
//                toReturn = advanceIndexAndGetNext();
//            }
//        }
//        return toReturn;
//    }

    private void resetIteratorToZero() {
//        indexOfNextElementIteratorWillRetrieve = 0;
        iterator = map.entrySet().iterator();
    }

//    @Override
//    public Iterator<Map.Entry<KEY, VALUE>> iterator() {
//        return this; // NOSONAR - this represents the iteration over the underlying collection as well
//    }

//    private Map.Entry<KEY, VALUE> advanceIndexAndGetNext() {
//        final boolean indexAtEndOfMap = indexOfNextElementIteratorWillRetrieve == this.map.size() - 1;
//        if (indexAtEndOfMap) {
//            // reset to beginning
//            indexOfNextElementIteratorWillRetrieve = 0;
//        } else {
//            indexOfNextElementIteratorWillRetrieve++;
//        }
//        return iterator.next();
//    }

}
