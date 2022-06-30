package io.confluent.csid.actors;

import io.confluent.csid.utils.TimeUtils;
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

    Greeter greeter = new Greeter();
    Actor<Greeter> actor = new Actor<>(TimeUtils.getClock(), greeter);

    static class Greeter {
        String prefix = "kiwi-";

        public String greet(String msg) {
            return prefix + msg;
        }
    }

    @Test
    void tell() {
        actor.tell(g -> g.greet("tell"));
    }

    @SneakyThrows
    @Test
    void ask() {
        Future<String> tell = actor.ask(g -> g.greet("tell"));
        actor.processBounded();
        String s = tell.get();
        assertThat(s).isEqualTo("kiwi-tell");
    }

    @Test
    void processBounded() {
    }

    @Test
    void processBlocking() {
    }
}