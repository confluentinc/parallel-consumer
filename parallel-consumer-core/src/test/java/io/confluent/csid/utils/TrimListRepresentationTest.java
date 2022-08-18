package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Antony Stubbs
 * @see TrimListRepresentation
 */
@Tag("transactions")
class TrimListRepresentationTest {

    @Test
    void customRepresentationFail() {
        List<Integer> one = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        List<Integer> two = IntStream.range(999, 2000).boxed().collect(Collectors.toList());
        assertThatThrownBy(() -> assertThat(one).withRepresentation(new TrimListRepresentation()).containsAll(two))
                .hasMessageContaining("trimmed");
    }

    @Test
    void customRepresentationPass() {
        Assertions.useRepresentation(new TrimListRepresentation());
        List<Integer> one = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        List<Integer> two = IntStream.range(0, 1000).boxed().collect(Collectors.toList());
        SoftAssertions all = new SoftAssertions();
        all.assertThat(one).containsAll(two);
        all.assertAll();
    }

}