package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SuppliersTest {

    @Test
    public void testMemoize() {
        UnderlyingSupplier underlyingSupplier = new UnderlyingSupplier();
        Supplier<Integer> memoizedSupplier = Suppliers.memoize(underlyingSupplier);
        assertEquals(0, underlyingSupplier.calls); // the underlying supplier hasn't executed yet
        assertEquals(5, (int) memoizedSupplier.get());

        assertEquals(1, underlyingSupplier.calls);
        assertEquals(5, (int) memoizedSupplier.get());

        assertEquals(1, underlyingSupplier.calls); // it still should only have executed once due to memoization
    }

    @Test
    public void testMemoizeNullSupplier() {
        assertThrows(NullPointerException.class, () -> Suppliers.memoize(null));
    }

    @Test
    public void testMemoizeSupplierReturnsNull() {
        assertThrows(NullPointerException.class, () -> {
            Supplier<?> supplier = Suppliers.memoize(() -> null);
            supplier.get();
        });
    }

    @Test
    public void testMemoizeExceptionThrown() {
        Supplier<Integer> memoizedSupplier = Suppliers.memoize(new ThrowingSupplier());
        assertThrows(IllegalStateException.class, memoizedSupplier::get);
    }

    private static class UnderlyingSupplier implements Supplier<Integer> {

        private int calls = 0;

        @Override
        public Integer get() {
            calls++;
            return calls * 5;
        }

    }

    private static class ThrowingSupplier implements Supplier<Integer> {

        @Override
        public Integer get() {
            throw new IllegalStateException();
        }
    }
}
