package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

public class LoopingResumingIteratorTest {

    @Test
    public void noFrom() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var entries = new LoopingResumingIterator<>(map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
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
        var entries = LoopingResumingIterator.build(2, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(2, 3, 0, 1);
    }


    @Test
    public void fromIsEnd() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var entries = LoopingResumingIterator.build(3, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(3, 0, 1, 2);
    }

    @Test
    public void fromBeingFirstElement() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var entries = LoopingResumingIterator.build(0, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2, 3);
    }

    @Test
    public void fromDoesntExist() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var entries = LoopingResumingIterator.build(88, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
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
            for (var x : entries) {
                results.add(x);
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2);
        }

        {
            var entries = LoopingResumingIterator.build(2, map);
            ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                var x = iterator.next();
                results.add(x);
            }
            if (entries.hasNext()) {
                iterator.next();
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(2, 0, 1);
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
            var iterator = entries.iterator();
            while (iterator.hasNext()) {
                var x = iterator.next();
                results.add(x);
            }
            Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2);


            if (entries.hasNext()) {
                Map.Entry<Integer, String> next = iterator.next();
                assertThat(next).isNotNull();
            }
        }
    }

    @Test
    void emptyCollection() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        var entries = LoopingResumingIterator.build(null, map);
        // several repeats as hasNext changes state
        Truth.assertThat(entries.hasNext()).isFalse();
        Truth.assertThat(entries.hasNext()).isFalse();
        Truth.assertThat(entries.hasNext()).isFalse();
    }

    @Test
    void emptyCollectionWithStartingPoint() {
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        var entries = LoopingResumingIterator.build(1, map);
        // several repeats as hasNext changes state
        Truth.assertThat(entries.hasNext()).isFalse();
        Truth.assertThat(entries.hasNext()).isFalse();
        Truth.assertThat(entries.hasNext()).isFalse();
    }
    
        @Test
    void emptyKeyIterator(){
        LinkedHashMap<Integer, String> map = new LinkedHashMap<>();
        map.put(0, "a");
        map.put(1, "b");
        map.put(2, "c");
        map.put(3, "d");
        var entries = LoopingResumingIterator.build(null, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
        }
        Map.Entry<Integer, String> next = entries.next();
        assertThat(next).isNull();
    }

}
