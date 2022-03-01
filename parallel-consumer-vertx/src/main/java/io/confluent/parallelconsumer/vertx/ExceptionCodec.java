package io.confluent.parallelconsumer.vertx;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExceptionCodec implements MessageCodec<RuntimeException, RuntimeException> {

    @Override
    public void encodeToWire(Buffer buffer, RuntimeException throwable) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public RuntimeException decodeFromWire(int pos, Buffer buffer) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public RuntimeException transform(RuntimeException throwable) {
        return throwable;
    }

    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
