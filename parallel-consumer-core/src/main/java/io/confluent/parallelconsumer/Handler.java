package io.confluent.parallelconsumer;

import java.util.function.Consumer;

public interface Handler<T> extends Consumer<T> {

}
