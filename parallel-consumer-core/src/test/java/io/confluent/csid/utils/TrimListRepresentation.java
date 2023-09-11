package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.presentation.StandardRepresentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Trims long lists to make large List assertions more readable
 */
@Slf4j
public class TrimListRepresentation extends StandardRepresentation {

    private final int sizeLimit = 10;

    protected static String msg = "Collection has been trimmed...";

    @Override
    public String toStringOf(Object raw) {
        if (raw instanceof Set) {
            var aSet = (Set<?>) raw;
            if (aSet.size() > sizeLimit)
                raw = new ArrayList<>(aSet);
        }
        if (raw instanceof Object[]) {
            Object[] anObjectArray = (Object[]) raw;
            if (anObjectArray.length > sizeLimit)
                raw = Arrays.stream(anObjectArray).collect(Collectors.toList());
        }
        if (raw instanceof String[]) {
            var anObjectArray = (String[]) raw;
            if (anObjectArray.length > sizeLimit)
                raw = Arrays.stream(anObjectArray).collect(Collectors.toList());
        }
        if (raw instanceof List) {
            List<?> aList = (List<?>) raw;
            if (aList.size() > sizeLimit) {
                log.trace("List too long ({}), trimmed...", aList.size());
                var trimmedListView = aList.subList(0, sizeLimit);
                // don't mutate backing lists
                var copy = new CopyOnWriteArrayList<Object>(trimmedListView);
                copy.add(msg);
                return super.toStringOf(copy);
            }
        }
        return super.toStringOf(raw);
    }
}
