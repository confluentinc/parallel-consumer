
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniLists;

import java.util.concurrent.ExecutionException;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Slf4j
public abstract class BrokerIntegrationTest<K, V> {

    static {
        System.setProperty("flogger.backend_factory", "com.google.common.flogger.backend.slf4j.Slf4jBackendFactory#getInstance");
    }

    int numPartitions = 1;

    String topic;

    @Getter
    private KafkaContainerManager kafkaContainerManager;

    @Getter
    protected KafkaClientUtils kcu;

    public BrokerIntegrationTest() {
        kafkaContainerManager = KafkaContainerManager.getSingleton();
        kcu = new KafkaClientUtils(kafkaContainerManager.getKafkaContainer());
    }

    @BeforeEach
    void open() {
        kcu.open();
    }

    @AfterEach
    void close() {
        kcu.close();
    }

    void setupTopic() {
        String name = LoadTest.class.getSimpleName();
        setupTopic(name);
    }

    protected String setupTopic(String name) {
        assertThat(kafkaContainerManager.getKafkaContainer().isRunning()).isTrue(); // sanity

        topic = name + "-" + nextInt();

        ensureTopic(topic, numPartitions);

        return topic;
    }

    protected void ensureTopic(String topic, int numPartitions) {
        NewTopic e1 = new NewTopic(topic, numPartitions, (short) 1);
        CreateTopicsResult topics = kcu.getAdmin().createTopics(UniLists.of(e1));
        try {
            Void all = topics.all().get();
        } catch (ExecutionException e) {
            // fine
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
