package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
class PCModuleTestEnv extends PCModule<String, String> {

    public PCModuleTestEnv(ParallelConsumerOptions<String, String> optionsInstance) {
        super(optionsInstance);
        var override = options().toBuilder()
                .consumer(mock(Consumer.class))
                .producer(mock(Producer.class))
                .build();
        super.optionsInstance = override;
    }

    @Override
    protected ProducerWrap<String, String> producerWrap() {
        return mockProducerWrapTransactional();
    }

    ProducerWrap mock = mock(ProducerWrap.class);

    {
        Mockito.when(mock.isConfiguredForTransactions()).thenReturn(true);

    }

    @NonNull
    private ProducerWrap mockProducerWrapTransactional() {
        return mock;
    }
}