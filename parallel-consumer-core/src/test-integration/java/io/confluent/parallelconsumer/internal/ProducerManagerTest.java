package io.confluent.parallelconsumer.internal;

import org.junit.jupiter.api.Test;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ManagedTruth.assertWithMessage;

/**
 * todo docs
 *
 * @author Antony Stubbs
 * @see ProducerManager
 */
class ProducerManagerTest {

    ProducerManager<Object, Object> pm = new ProducerManager<>();

    /**
     * todo docs
     */
    @Test
    void sendingGetsLockedInTx() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var notBlockedSends = pm.produceMessages();
        assertThat(notBlockedSends).hasSize(-1);

        // pretend to start to commit
        pm.preAcquireWork();

        //
        assertThat(pm).isTransactionCommittingInProgress();

        // these futures should block
        var blockedSends = pm.produceMessages();
        assertThat(blockedSends).hasSize(-1);

        // pretend to finish tx
        pm.postCommit();

        //
        assertThat(pm).isNotTransactionCommittingInProgress();


        // blocked to send should only now complete
        assertThat(blockedSends).hasSize(-1);
    }

    /**
     * todo docs
     */
    @Test
    void txOnlyStartedUponMessageSend() {
        assertThat(pm).isNotTransactionCommittingInProgress();

        assertWithMessage("Transaction is started as not open")
                .that(pm)
                .transactionNotOpen();

        {
            var notBlockedSends = pm.produceMessages();
        }

        assertThat(pm).transactionOpen();

        {
            var notBlockedSends = pm.produceMessages();
        }

        pm.preAcquireWork();

        assertThat(pm).isTransactionCommittingInProgress();

        pm.commitOffsets(null, null);

        assertThat(pm).isTransactionCommittingInProgress();

        pm.postCommit();

        assertThat(pm).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(pm)
                .transactionNotOpen();
    }

}