package org.example.kafkaoffset;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MultipleKafkaOperatorsApp {
    private static Logger logger = LoggerFactory.getLogger(MultipleKafkaOperatorsApp.class);

    public static void main(String[] args) {
        KafkaPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaPipelineOptions.class);
        options.setJobName("MultipleKafkaOperatorsApp");
        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromKafka", KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBrokers())
                .withTopics(Arrays.asList("input"))
                .withConsumerConfigUpdates(new ImmutableMap.Builder<String, Object>()
                        .put(ConsumerConfig.GROUP_ID_CONFIG, "MultipleKafkaOperatorsApp")
                        .build())
                .commitOffsetsInFinalize()
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
                .apply("Print Metadata", ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c) {
                        logger.info("Topic:" + c.element().getTopic() +
                                "Partition:" + c.element().getPartition() +
                                "Offset:" + c.element().getOffset());
                        c.output(c.element().getKV());
                    }
                }))
                .apply("Window", Window.<KV<String, String>>into(new GlobalWindows())
                        .triggering(AfterPane.elementCountAtLeast(1))
                        .discardingFiredPanes()
                        .withAllowedLateness(Duration.ZERO))
                .apply("State", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @StateId("state")
                    private final StateSpec<ValueState<String>> leftState = StateSpecs.value();
                    @TimerId("gcTimer")
                    private final TimerSpec leftStateExpiryTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                    @ProcessElement
                    public void process(
                            ProcessContext c,
                            @Timestamp Instant ts,
                            @StateId("state") ValueState<String> state,
                            @TimerId("gcTimer") Timer gcTimer) {
                        // Set the timer to be 2 minutes after the maximum timestamp seen. This will
                        // keep overwriting the same timer, so
                        // as long as there is activity on this key the state will stay active. Once the
                        // key goes inactive for 2 minutes's
                        // worth of event time (as measured by the watermark), then the gc timer will
                        // fire.
                        Instant expirationTime = new Instant(ts.getMillis()).plus(Duration.standardSeconds(60));
                        c.output(KV.of("", "MultipleKafkaOperatorApp" + c.element().getValue()));
                        state.write(c.element().getValue());
                        gcTimer.set(expirationTime);
                    }

                    @OnTimer("gcTimer")
                    public void onLeftCollectionStateExpire(OnTimerContext c,
                            @StateId("state") ValueState<String> state) {
                        c.output(KV.of("", "MultipleKafkaOperatorApp State" + state.read()));
                        state.clear();
                    }
                }

                )).setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .apply("Write to Kafka", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.getKafkaBrokers())
                        .withTopic("output")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        p.apply("ReadFromKafka 2", KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBrokers())
                .withTopics(Arrays.asList("dxb.input"))
                .withConsumerConfigUpdates(new ImmutableMap.Builder<String, Object>()
                        .put(ConsumerConfig.GROUP_ID_CONFIG, "MultipleKafkaOperatorsApp")
                        .build())
                .commitOffsetsInFinalize()
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
                .apply("Print Metadata 2", ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c) {
                        logger.info("Topic:" + c.element().getTopic() +
                                "Partition:" + c.element().getPartition() +
                                "Offset:" + c.element().getOffset());
                        c.output(c.element().getKV());
                    }
                }))
                .apply("Window 2", Window.<KV<String, String>>into(new GlobalWindows())
                        .triggering(AfterPane.elementCountAtLeast(1))
                        .discardingFiredPanes()
                        .withAllowedLateness(Duration.ZERO))
                .apply("Write to Kafka 2", KafkaIO.<String, String>write()
                        .withBootstrapServers(options.getKafkaBrokers())
                        .withTopic("output")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));

        p.run().waitUntilFinish();

    }
}
