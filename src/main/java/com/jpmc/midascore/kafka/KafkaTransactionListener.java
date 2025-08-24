package com.jpmc.midascore.kafka;
import com.jpmc.midascore.foundation.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class KafkaTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionListener.class);

    // Optional buffer to help you read the first few transactions without a debugger.
    private final List<Transaction> received = new CopyOnWriteArrayList<>();

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core",
        containerFactory = "transactionKafkaListenerContainerFactory"
    )
    public void onMessage(Transaction tx) {
        // For visibility while running tests:
        log.info("Received transaction: id={}, amount={}", tx.getId(), tx.getAmount());
        if (received.size() < 10) { // don’t let it grow forever in tests
            received.add(tx);
        }
    }

    // Helper for local inspection (not used by tests, but handy):
    public List<Transaction> getReceived() {
        return received;
    }
}
