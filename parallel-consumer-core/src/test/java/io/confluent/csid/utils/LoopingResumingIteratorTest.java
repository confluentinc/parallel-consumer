package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import com.google.common.truth.Truth8;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Antony Stubbs
 * @see LoopingResumingIterator
 */
class LoopingResumingIteratorTest {

    @Test
    void noFrom() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var iterator = new LoopingResumingIterator<>(map);

        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x.get());
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2, 3);
    }

    @Test
    public void fromInMiddle() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var iterator = LoopingResumingIterator.build(2, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x.get());
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(2, 3, 0, 1);
    }


    @Test
    void fromIsEnd() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var iterator = LoopingResumingIterator.build(3, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x.get());
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(3, 0, 1, 2);
    }

    @Test
    public void fromBeginningFirstElement() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var iterator = LoopingResumingIterator.build(0, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x.get());
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2, 3);
    }

    @Test
    void fromDoesntExist() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var iterator = LoopingResumingIterator.build(88, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x.get());
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2, 3);
    }

    @Test
    void loopsCorrectly() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        {
            var entries = LoopingResumingIterator.build(null, map);
            ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
            var iterator = entries;
            for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
                results.add(x.get());
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2);
        }

        {
            var entries = LoopingResumingIterator.build(2, map);
            ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
            var iterator = entries;
            for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
                results.add(x.get());
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(2, 0, 1);

            // check reports now empty
            Truth8.assertThat(entries.next()).isEmpty();
        }
    }

    /**
     * Like {@link #loopsCorrectly()}, but sets th initial starting element as the first element
     */
    @Test
    void loopsCorrectlyWithStartingObjectIndexZero() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        {
            var entries = LoopingResumingIterator.build(0, map);
            ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
            var iterator = entries;
            for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
                results.add(x.get());
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2);

            // check reports now empty
            {
                Truth8.assertThat(entries.next()).isEmpty();
            }
        }
    }

    @Test
    void emptyInitialStartingKey() {
        //
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");

        //
        var iterator = LoopingResumingIterator.build(null, map);

        //
        var next = iterator.next().get();
        Truth.assertThat(next.getKey()).isEqualTo(0);
        Truth.assertThat(next.getValue()).isEqualTo("a");
    }

    /**
     * Exposes an issue where because of iterators being reset, the two iteration passes effectively see a different
     * collection, through the two different iterators - so the second iterator might be missing the start key
     * <p>
     * See https://github.com/confluentinc/parallel-consumer/pull/435
     *
     * @see ConcurrentHashMap section about iterators seeing a snapshot of the map state from time of creation
     */
    @Test
    void demoOfNoSuchElementIssue() {
        final int INDEX_TO_REMOVE = 4;
        Map<Integer, String> map = new ConcurrentHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        map.put(INDEX_TO_REMOVE, "e");
        map.put(5, "f");
        map.put(6, "g");
        map.put(7, "h");
        map.put(8, "i");
        map.put(9, "j");

        ArrayList<Optional<Map.Entry<Integer, String>>> results = new ArrayList<>();

        var iterator = LoopingResumingIterator.build(INDEX_TO_REMOVE, map);

        // iterate, and remove the staring element
        for (var x = iterator.next(); x.isPresent(); x = iterator.next()) {
            results.add(x);
            if (x.get().getKey() == INDEX_TO_REMOVE) {
                map.remove(INDEX_TO_REMOVE);
            }
        }

        var expected = new LinkedHashMap<Integer, String>();
        expected.put(4, "e");
        expected.put(5, "f");
        expected.put(6, "g");
        expected.put(7, "h");
        expected.put(8, "i");
        expected.put(9, "j");
        expected.put(0, "a");
        expected.put(1, "b");
        expected.put(2, "c");
        expected.put(3, "d");
        var expectedAsList = expected.entrySet().stream().map(Optional::of).collect(Collectors.toList());

        Truth.assertThat(results).containsExactlyElementsIn(expectedAsList).inOrder();

        Truth8.assertThat(iterator.next()).isEmpty();

        map.clear();

        Truth8.assertThat(iterator.next()).isEmpty();
    }

}
