package org.example.kafkaoffset;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoStateApp {
    private static Logger logger = LoggerFactory.getLogger(NoStateApp.class);

    public static void main(String[] args) {
        KafkaPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaPipelineOptions.class);
        options.setJobName("NoStateApp");
        Pipeline p = Pipeline.create(options);

        p.apply("Generate Records", GenerateSequence.from(0).withRate(1,Duration.standardSeconds(30)))
                .apply("State", ParDo.of(new DoFn<Long, KV<String,String>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c
                    ) {
                        c.output(KV.of("","NoStateApp" + c.element().toString()));
                    }
                }))
                .apply("Write to Kafka",KafkaIO.<String,String>write()
                        .withBootstrapServers(options.getKafkaBrokers())
                        .withTopic("output")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        p.run().waitUntilFinish();

    }
}
