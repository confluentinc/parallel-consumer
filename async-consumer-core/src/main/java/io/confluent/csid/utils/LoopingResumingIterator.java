package io.confluent.csid.utils;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Consumer;

/**
 * Loop implementations that will resume from a given key. Can be constructed and used as an iterable, or a function
 * passed into the static version {@link #iterateStartingFromKeyLooping}.
 *
 * The non functional version is useful when you want to use looping constructs such as {@code break} and {@code continue}.
 */
public class LoopingResumingIterator<KEY, VALUE> implements Iterator<Map.Entry<KEY, VALUE>>, Iterable<Map.Entry<KEY, VALUE>> {

    private Iterator<Map.Entry<KEY, VALUE>> iterator;

    @Getter
    private final Optional<KEY> iterationStartingPoint;

    private final Map<KEY, VALUE> map;

    private boolean looped = false;

    private Optional<Integer> foundIndex = Optional.empty();

    private int indexOfNextElementToRetrieve = 0;

    private final boolean stillIterateCollectionIfStartingPointDoesntExist = true;

    public LoopingResumingIterator(KEY startingKey, Map<KEY, VALUE> map) {
        this.iterationStartingPoint = Optional.ofNullable(startingKey);
        this.map = map;
        this.iterator = map.entrySet().iterator();
    }

    public LoopingResumingIterator(Map<KEY, VALUE> map) {
        this(null, map);
    }

    @Override
    public boolean hasNext() {
        if (iterationStartingPoint.isEmpty()) return iterator.hasNext();
        if (looped) {
            if (foundIndex.orElse(-1) == indexOfNextElementToRetrieve) {
                // we've looped around
                return false;
            } else {
                return true;
            }
        } else {
            boolean atEndOfFirstIteration = !iterator.hasNext();
            if (atEndOfFirstIteration) {
                looped = true;
                if (foundIndex.isPresent() && foundIndex.get() != 0) {
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
    public Map.Entry<KEY, VALUE> next() {
        Map.Entry<KEY, VALUE> toReturn = null;
        if (iterationStartingPoint.isEmpty()) {
            toReturn = getNext();
        } else {
            Object lookingFor = iterationStartingPoint.get();
            if (foundIndex.isPresent()) {
                if (looped) {
                    toReturn = getNext();
                    if (toReturn.getKey() == lookingFor) {
                        // back at the beginning
                        throw new RuntimeException("nope.. cant return false to has next without actually getting next?");
                    }
                } else {
                    // still on first pass
                    if (iterator.hasNext()) {
                        toReturn = getNext();
                    } else {
                        // reached end of first pass
                        looped = true;
                        resetIterator();
                        // return first looped value
                        toReturn = getNext();
                    }
                }
            } else {
                // find the starting point
                while (iterator.hasNext()) {
                    Map.Entry<KEY, VALUE> next = getNext();
                    if (next.getKey() == lookingFor) {
                        foundIndex = Optional.of(indexOfNextElementToRetrieve - 1);
                        toReturn = next;
                        break;
                    }
                }
                if (foundIndex.isEmpty() && stillIterateCollectionIfStartingPointDoesntExist) {
                    foundIndex = Optional.of(0); // act as if it was found at 0 and proceed normally
                    resetIterator();
                    toReturn = getNext();
                }
            }
        }
        return toReturn;
    }

    private void resetIterator() {
        this.indexOfNextElementToRetrieve = 0;
        iterator = map.entrySet().iterator();
    }

    private Map.Entry<KEY, VALUE> getNext() {
        indexOfNextElementToRetrieve++;
        return iterator.next();
    }

    @Override
    public Iterator<Map.Entry<KEY, VALUE>> iterator() {
        return this;
    }

    /**
     * Simpler alternative that uses an embedded function
     */
    static <KEY, VALUE> void iterateStartingFromKeyLooping(Optional<KEY> key, LinkedHashMap<KEY, VALUE> map, Consumer<Map.Entry<KEY, VALUE>> c) {
        if (key.isPresent()) {
            boolean found = false;
            // find starting point and iterate
            for (Map.Entry<KEY, VALUE> entry : map.entrySet()) {
                if (!found && !key.equals(entry.getKey())) {
                    continue;
                }
                found = true;
                c.accept(entry);
            }
        }
        // start from beginning now, up until starting point
        for (Map.Entry<KEY, VALUE> entry : map.entrySet()) {
            if (key.equals(entry.getKey())) {
                break;
            }
            c.accept(entry);
        }
    }
}