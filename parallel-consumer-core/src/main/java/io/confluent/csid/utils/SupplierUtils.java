package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import lombok.experimental.UtilityClass;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@UtilityClass
public class SupplierUtils {

    public static <T> Supplier<T> memoize(Supplier<T> delegate) {
        Objects.requireNonNull(delegate);
        AtomicReference<T> value = new AtomicReference<>();
        ReentrantLock lock = new ReentrantLock();
        return () -> {
            T val = value.get();
            if (val == null) {
                lock.lock();
                try {
                    val = value.get();
                    if (val == null) {
                        val = Objects.requireNonNull(delegate.get());
                        value.set(val);
                    }
                } finally {
                    lock.unlock();
                }
            }
            return val;
        };
    }
}
