package io.confluent.csid.actors;

import io.confluent.parallelconsumer.ManagedTruth;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Sanity tests for {@link CompletableFuture}.
 *
 * @author Antony Stubbs
 */
class CompletableFutureTest {

    @SneakyThrows
    @Test
    void sanity() {
        var base = new CompletableFuture<String>();
        Function<String, String> appenderFunction = s -> s + "1";
        var appended = base.thenApply(appenderFunction);
        var a = base.complete("a");
        var s = appended.get();
        ManagedTruth.assertThat(s).isEqualTo("a1");
    }

}
