package io.confluent.csid.actors;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.TimeUtils;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

/**
 * @see Actor
 */
@Slf4j
class ActorTest {

    public static final String MESSAGE = "tell";
    Greeter greeter = new Greeter();
    Actor<Greeter> actor = new Actor<>(TimeUtils.getClock(), greeter);

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
        actor.processBounded();
        //        ManagedTruth.assertThat(greeter). // todo get TG working with Greeter class
        assertThat(greeter.getTold()).isEqualTo(MESSAGE);

    }

    @SneakyThrows
    @Test
    void ask() {
        Future<String> tell = actor.ask(g -> g.greet(MESSAGE));
        actor.processBounded();
        String s = tell.get();
        assertThat(s).isEqualTo(Greeter.PREFIX + MESSAGE);
    }

    @Test
    void processBlocking() {
        //todo
    }
}