package org.hyland.contentlake.connector;

import org.springframework.beans.factory.ObjectProvider;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Minimal {@link ObjectProvider}s for the connector tests: the iterable and {@code getIfAvailable} halves,
 * which is all the classes under test use. A fresh iterator per call, so a component that iterates twice
 * sees the same beans.
 */
final class TestProviders {

    private TestProviders() {
    }

    @SafeVarargs
    static <T> ObjectProvider<T> of(T... values) {
        List<T> present = Arrays.stream(values).filter(java.util.Objects::nonNull).toList();
        return new ObjectProvider<>() {

            @Override
            public Iterator<T> iterator() {
                return present.iterator();
            }

            @Override
            public T getObject() {
                if (present.size() != 1) {
                    throw new UnsupportedOperationException();
                }
                return present.getFirst();
            }

            @Override
            public T getObject(Object... args) {
                return getObject();
            }

            @Override
            public T getIfAvailable() {
                return present.isEmpty() ? null : present.getFirst();
            }

            @Override
            public T getIfUnique() {
                return present.size() == 1 ? present.getFirst() : null;
            }
        };
    }

    /** An empty provider, for the application that has none of whatever this is. */
    static <T> ObjectProvider<T> none() {
        return of();
    }
}
