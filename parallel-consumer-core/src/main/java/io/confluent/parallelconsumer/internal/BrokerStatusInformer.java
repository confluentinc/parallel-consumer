package io.confluent.parallelconsumer.internal;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
public interface BrokerStatusInformer {

    void addStatusListener(BrokerStatusListener listener);

    BrokerStatus getConnectionStatus();

    interface BrokerStatusListener {
        void notifyBrokerStatus(BrokerStatus status);
    }

    enum BrokerStatus {
        CONNECTED, DISCONNECTED
    }
}
