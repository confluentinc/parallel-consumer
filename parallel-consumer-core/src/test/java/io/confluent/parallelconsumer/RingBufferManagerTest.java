package io.confluent.parallelconsumer;

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
class RingBufferManagerTest extends ParallelEoSStreamProcessorTestBase {

    @Test
    void testWorkManagement() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumerSpy)
                .build();
        var pc = new ParallelEoSStreamProcessor<String, String>(options);
        var wm = new WorkManager<>(options, consumerSpy);

        int expected = 100;

        List<ConsumerRecord<String, String>> consumerRecords = ktu.generateRecordsForKey(1, expected);
        ktu.send(consumerSpy, consumerRecords);

        BrokerPollSystem<String, String> stringStringBrokerPollSystem = new BrokerPollSystem<>(new ConsumerManager<>(consumerSpy), wm, pc, options);
        stringStringBrokerPollSystem.start();

//        ArrayBlockingQueue<Runnable> buffer = new ArrayBlockingQueue<>(options.getNumberOfThreads() * 3);
        LinkedBlockingQueue<Runnable> buffer = new LinkedBlockingQueue<>();

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(options.getNumberOfThreads(), options.getNumberOfThreads(),
                0L, MILLISECONDS,
                buffer);


        Consumer<ConsumerRecord<String, String>> usersFunction = (rec) -> {
            log.info("user func sleep {}", rec.offset());
            try {
                Thread.sleep(RandomUtils.nextInt(1000, 5000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("user func end");
        };
        Consumer<String> callback = (astring) -> {
            log.info("user callback");
        };

//        rb.startRingBuffer(buffer, usersFunction, callback);

//        var rb = new RingBufferManager<String, String>(options, wm, pc, threadPoolExecutor);
        pc.poll(usersFunction);

        await().untilAsserted(() -> {
            assertThat(pc.getWorkMailBox()).hasSize(expected);
        });
    }

    @Test
    void testFullCycle() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .numberOfThreads(1000)
                .build();
//        var pc = new ParallelEoSStreamProcessor<String, String>(options);
//        var wm = new WorkManager<>(options, consumerSpy);
        setupParallelConsumerInstance(options);

        BrokerPollSystem.setLongPollTimeout(Duration.ofSeconds(2));

        int expected = 1_000_000;

        List<ConsumerRecord<String, String>> consumerRecords = ktu.generateRecordsForKey(1, expected);
        ktu.send(consumerSpy, consumerRecords);


//        BrokerPollSystem<String, String> stringStringBrokerPollSystem = new BrokerPollSystem<>(new ConsumerManager<>(consumerSpy), wm, pc, options);
//        stringStringBrokerPollSystem.start();
//
////        ArrayBlockingQueue<Runnable> buffer = new ArrayBlockingQueue<>(options.getNumberOfThreads() * 3);
//        LinkedBlockingQueue<Runnable> buffer = new LinkedBlockingQueue<>();
//
//        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(options.getNumberOfThreads(), options.getNumberOfThreads(),
//                0L, MILLISECONDS,
//                buffer);
//

        Consumer<ConsumerRecord<String, String>> usersFunction = (rec) -> {
//            log.info("user func sleep {}", rec.offset());
            try {
//                Thread.sleep(RandomUtils.nextInt(20, 200));
                Thread.sleep(0);
//                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            log.info("user func end");
        };
        Consumer<String> callback = (astring) -> {
            log.info("user callback");
        };

//        rb.startRingBuffer(buffer, usersFunction, callback);

//        var rb = new RingBufferManager<String, String>(options, wm, pc, threadPoolExecutor);
        log.info("Starting");
        parallelConsumer.poll(usersFunction);

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expected);

        Assertions.useRepresentation(new TrimListRepresentation());
        await().atMost(ofSeconds(60))
                .untilAsserted(() -> {
                    List<Map<TopicPartition, OffsetAndMetadata>> commitHistoryInt = consumerSpy.getCommitHistoryInt();
                    assertThat(commitHistoryInt).isNotEmpty();
                    long offset = commitHistoryInt.get(commitHistoryInt.size() - 1).entrySet().stream().findFirst().get().getValue().offset();
                    bar.stepTo(successfulWork.size());// is going to dance around
                    assertThat(offset).isEqualTo(expected);
                });
        bar.stepTo(expected);
        bar.close();

        parallelConsumer.close();
    }

}
