package io.confluent.parallelconsumer.vertx;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

@Slf4j
public class ArrayListCodec implements MessageCodec<ArrayList, ArrayList> {

    @Override
    public void encodeToWire(Buffer buffer, ArrayList arrayList) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public ArrayList decodeFromWire(int pos, Buffer buffer) {
        throw new IllegalStateException("Not implemented");

    }

    @Override
    public ArrayList transform(ArrayList arrayList) {
        return arrayList;
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
