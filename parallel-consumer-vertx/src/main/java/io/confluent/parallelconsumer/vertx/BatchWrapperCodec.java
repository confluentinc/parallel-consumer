package io.confluent.parallelconsumer.vertx;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.codecs.JsonArrayMessageCodec;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;

class BatchWrapperCodec<K, V> implements MessageCodec<BatchWrapper<K, V>, BatchWrapper<K, V>> {

    private final JsonArrayMessageCodec delegate = new JsonArrayMessageCodec();

    @Override
    public void encodeToWire(Buffer buffer, BatchWrapper kvBatchWrapper) {
        String encode = Json.encode(kvBatchWrapper);
        buffer.appendString(encode);
    }

    @Override
    public BatchWrapper<K, V> decodeFromWire(int pos, Buffer buffer) {
        JsonArray objects = delegate.decodeFromWire(pos, buffer);
        Object[] objects1 = objects.stream().toArray();
        return new BatchWrapper(null);
    }

    @Override
    public BatchWrapper<K, V> transform(BatchWrapper<K, V> kvBatchWrapper) {
        return kvBatchWrapper;
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