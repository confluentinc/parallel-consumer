package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;


/**
 * @see Actor
 */
@Slf4j
class ActorTest {

    public static final String MESSAGE = "tell";
    Greeter greeter = new Greeter();
    Actor<Greeter> actor = new Actor<>(greeter);

    @Data
    public static class Greeter {
        public static final String PREFIX = "kiwi-";
        String told = "";

        public String greet(String msg) {
            return PREFIX + msg;
        }
    }

    @Test
    void tell() {
        actor.tell(g -> g.setTold(MESSAGE));
        actor.process();
//        ManagedTruth.assertThat(greeter). // todo get TG working with Greeter class
        assertThat(greeter.getTold()).isEqualTo(MESSAGE);
    }

    @SneakyThrows
    @Test
    void ask() {
        Future<String> tell = actor.ask(g -> g.greet(MESSAGE));
        actor.process();
        String s = tell.get();
        assertThat(s).isEqualTo(Greeter.PREFIX + MESSAGE);
    }

    @SneakyThrows
    @Test
    void close() {
        Future<String> tell = actor.ask(g -> g.greet(MESSAGE));
        actor.close();
        String s = tell.get();
        assertThat(s).isEqualTo(Greeter.PREFIX + MESSAGE);

        Assertions.assertThatThrownBy(() -> actor.tell(g -> g.greet("closed")))
                .hasMessageContainingAll("CLOSED", "not", "target", "state");
    }

    @Test
    void tellImmediately() {
        actor.tellImmediately(g -> g.setTold("1"));
        actor.tellImmediately(g -> g.setTold("2"));
        actor.close();
        assertThat(greeter.getTold())
                .isEqualTo("1");
    }

    @SneakyThrows
    @Test
    void processBlocking() {
        final int delayMs = 1000;
        final String magic = "magic";
        final CountDownLatch returnedAfterBlocking = new CountDownLatch(1);
        var pool = Executors.newCachedThreadPool();

        // setup 1s delay trigger
        pool.submit(() -> {
            // delay, so we block polling for a second
            try {
                Thread.sleep(delayMs + 100); // add 100ms for fuzz factor / race condition
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // send the message to wake up the actor from the blocking process
            actor.tell(a -> a.setTold(magic));
        });

        //
        pool.submit(() -> {
            try {
                actor.processBlocking(Duration.ofMinutes(1)); // effectively infinite
                returnedAfterBlocking.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // await AT LEAST delayMs before condition passes (earlier would mean there was no block during processing)
        // trigger should fire after the delay, waking of the process block and processing the closure to set the value
        Awaitility.await()
                .atLeast(Duration.ofMillis(delayMs))
                .untilAsserted(() -> assertThat(greeter.getTold()).isEqualTo(magic));

        //
        Truth.assertWithMessage("Thread returned from blocking process ok")
                .that(returnedAfterBlocking.getCount()).isEqualTo(0);
    }
}