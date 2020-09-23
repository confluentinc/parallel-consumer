package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
        var entries = new LoopingResumingIterator<>(2, map);
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
        var entries = new LoopingResumingIterator<>(3, map);
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
        var entries = new LoopingResumingIterator<>(0, map);
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
        var entries = new LoopingResumingIterator<>(88, map);
        ArrayList<Map.Entry<Integer, String>> results = new ArrayList<>();
        for (var x : entries) {
            results.add(x);
        }
        Assertions.assertThat(results).extracting(Map.Entry::getKey).containsExactly(0, 1, 2, 3);
    }
}
