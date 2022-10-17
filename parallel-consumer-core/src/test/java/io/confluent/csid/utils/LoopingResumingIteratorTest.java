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
//                Truth.assertThat(entries.hasNext()).isFalse();
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
//                Truth.assertThat(entries.hasNext()).isFalse();
                Truth8.assertThat(entries.next()).isEmpty();
            }
        }
    }
//
//    @Test
//    void emptyCollection() {
//        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
//        var entries = LoopingResumingIterator.build(null, map);
//        // several repeats as hasNext changes state
//        Truth.assertThat(entries.hasNext()).isFalse();
//        Truth.assertThat(entries.hasNext()).isFalse();
//        Truth.assertThat(entries.hasNext()).isFalse();
//    }
//
//    @Test
//    void emptyCollectionWithStartingPoint() {
//        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
//        var entries = LoopingResumingIterator.build(1, map);
//        // several repeats as hasNext changes state
//        Truth.assertThat(entries.hasNext()).isFalse();
//        Truth.assertThat(entries.hasNext()).isFalse();
//        Truth.assertThat(entries.hasNext()).isFalse();
//    }

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
//
//    /**
//     * Example issue where an element is removed from the underlaying collection during iteration
//     * <p>
//     * See https://github.com/confluentinc/parallel-consumer/pull/435
//     */
//    @Test
//    void demoOfNoSuchElementIssue() {
//        final int INDEX_TO_REMOVE = 4;
//        Map<Integer, String> map = new ConcurrentHashMap<>();
//        map.put(0, "a");
//        map.put(1, "b");
////        map.put(2, "c");
////        map.put(3, "d");
////        map.put(INDEX_TO_REMOVE, "e");
////        map.put(5, "f");
////        map.put(6, "g");
////        map.put(7, "h");
////        map.put(8, "i");
////        map.put(9, "j");
//
//        var iterator = LoopingResumingIterator.build(1, map);
//
////        // remove the last element
//        Map.Entry<Integer, String> last = null;
//        while (iterator.hasNext()) {
//            last = iterator.next();
//            if (last.getKey() == INDEX_TO_REMOVE) {
//                map.remove(INDEX_TO_REMOVE);
//                break;
//            }
//        }
//
//        Truth.assertThat(iterator.hasNext()).isTrue();
//
////        map.clear();
//
//        Truth.assertThat(iterator.next()).isNull();
//
//        Truth.assertThat(iterator.hasNext()).isFalse();
//    }

}
