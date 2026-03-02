package org.msse.demo.phase3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("InfiniteLoopStatement")
public class InStateStreamersVsEventCapacity {
    public static void main(String[] args) {
        // define the properties for the Kafka consumer
        Properties props = new Properties();

        // where does the consumer connect to Kafka (the "bootstrap server(s)")?
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // what types of data will the consumer receive for the key and value of the records?
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // what group id should Kafka use to identify this consumer?
        // this allows the consumer to keep track of its progress and also have other consumers process in parallel
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "phase3-instate");
        // create a Kafka consumer instance with the properties above
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // subscribe the consumer to the topic(s) it will be consuming from
        consumer.subscribe(Arrays.asList("data-demo-tickets", "data-demo-events", "data-demo-streams", "data-demo-venues"));

        // start polling for messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(
                        "RECORD RECEIVED: " +
                                "key = " + record.key() + ", " +
                                "value = " + record.value() + ", " +
                                "partition = " + record.partition() + ", " +
                                "offset = " + record.offset()
                );

                // process the received message here
            }

            // commit offsets of the processed records (keeping track of what the consumer has already seen)
            consumer.commitSync();
        }
    }
}
