package org.example.kafkaoffset;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface KafkaPipelineOptions extends FlinkPipelineOptions {
    @Description("Kafka Brokers")
    @Default.String("kafka-cp-kafka-headless:9092")
    String getKafkaBrokers();

    void setKafkaBrokers(String value);
}
