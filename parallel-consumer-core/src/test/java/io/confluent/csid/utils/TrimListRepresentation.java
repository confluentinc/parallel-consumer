package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.presentation.StandardRepresentation;
import org.testcontainers.shaded.com.google.common.collect.Lists;

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
    public String toStringOf(Object o) {
        if (o instanceof Set) {
            Set aSet = (Set) o;
            if (aSet.size() > sizeLimit)
                o = aSet.stream().collect(Collectors.toList());
        }
        if (o instanceof Object[]) {
            Object[] anObjectArray = (Object[]) o;
            if (anObjectArray.length > sizeLimit)
                o = Arrays.stream(anObjectArray).collect(Collectors.toList());
        }
        if (o instanceof String[]) {
            Object[] anObjectArray = (Object[]) o;
            if (anObjectArray.length > sizeLimit)
                o = Arrays.stream(anObjectArray).collect(Collectors.toList());
        }
        if (o instanceof List) {
            List<?> aList = (List<?>) o;
            if (aList.size() > sizeLimit) {
                log.trace("List too long ({}), trimmed...", aList.size());
                List trimmedListView = aList.subList(0, sizeLimit);
                // don't mutate backing lists
                CopyOnWriteArrayList copy = Lists.newCopyOnWriteArrayList(trimmedListView);
                copy.add(msg);
                return super.toStringOf(copy);
            }
        }
        return super.toStringOf(o);
    }
}
