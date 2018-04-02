package uk.co.bitcat.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import uk.co.bitcat.dto.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TransactionConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public TransactionConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
    }

    public List<Transaction> receive() {
        List<Transaction> transactions = new ArrayList<>();

        ConsumerRecords<String, String> records = consumer.poll(5000);

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            ObjectMapper mapper = new ObjectMapper();

            try {
                Transaction tx = mapper.readValue(record.value(), Transaction.class);
                transactions.add(tx);
            } catch (IOException e) {
                System.err.println("Unable to parse received record: " + record.value() + "\n"
                    + "Exception: " + e.getMessage());
            }
        }

        return transactions;
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
