package io.vepo.kafka.stream.delay;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class KafkaStream implements Closeable {

    private static final String applicationId = "wordcount-application";
    private final String outputTopic = "WordsWithCountsTopic";
    private final String inputTopic = "TextLinesTopic";
    private final KafkaStreams streams;
    private final CountDownLatch latch;

    public KafkaStream(String bootstrapServers) {
        latch = new CountDownLatch(1);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(inputTopic);
        KTable<String, Long> wordCounts = textLines.flatMapValues(textLine -> {
            System.out.println("Flat Map: " + textLine);
            return Arrays.asList(textLine.toLowerCase()
                                         .split("\\W+"));
        })
                                                   .groupBy((key, word) -> {
                                                       System.out.println("Grouping: key=" + key + " work=" + word);
                                                       return word;
                                                   })
                                                   .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

    }

    public void start() {
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String inputTopic() {
        return inputTopic;
    }

    public String outputTopic() {
        return outputTopic;
    }
    
    public static String applicationId() {
        return applicationId;
    }

    @Override
    public void close() {
        latch.countDown();
        streams.close();
    }
}