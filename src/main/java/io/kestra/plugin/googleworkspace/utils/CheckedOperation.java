package io.kestra.plugin.googleworkspace.utils;

import io.kestra.core.utils.Rethrow;

import java.util.Objects;

public interface CheckedOperation<T, R, E extends Exception> extends Rethrow.FunctionChecked<T, R, E> {

    default <V> CheckedOperation<T, V, E> andThen(CheckedOperation<? super R, ? extends V, E> after) {
        Objects.requireNonNull(after);
        return (T t) -> after.apply(apply(t));
    }
}
