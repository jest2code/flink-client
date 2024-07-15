package org.appian.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.appian.common.Message;

import java.time.Instant;

public class FlinkJobFilter {

  public void start() throws Exception {
    System.out.println("Executing Flink job using Filter Operator...");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("flinktopic")
        .setGroupId("flink-group")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

    DataStream<Message> messageStream = kafkaStream.map(new JsonToEventMapFunction());

    DataStream<Message> contractStream = messageStream
        .filter(message -> message.getType().equals("Contract"));

    DataStream<Message> eMessageStream = messageStream
        .filter(message -> message.getType().equals("eMessage"));

    DataStream<Message> worksheetStream = messageStream
        .filter(message -> message.getType().equals("Worksheet"));

    contractStream
        .keyBy(m -> m.getType() + "-" + m.getCaseId())
        .process(new MessageRouterProcessFunction())
        .setParallelism(3);

    eMessageStream
        .keyBy(m -> m.getType() + "-" + m.getCaseId())
        .process(new MessageRouterProcessFunction())
        .setParallelism(3);

    worksheetStream
        .keyBy(m -> m.getType() + "-" + m.getCaseId())
        .process(new MessageRouterProcessFunction())
        .setParallelism(3);

    env.execute("Executing stream processor with filter");
  }

  private static class MessageRouterProcessFunction extends KeyedProcessFunction<String, Message, Void> {
    @Override
    public void processElement(Message message, KeyedProcessFunction<String, Message, Void>.Context context, Collector<Void> collector) throws Exception {
      System.out.println(
          Instant.now() + ":: Thread ID: " + Thread.currentThread().getId() + ", Processing Started Id: " +
              message.getId() + ", Type: " + message.getType() + ", CaseId: " + message.getCaseId());

      Thread.sleep(2000);
      System.out.println(
          Instant.now() + ":: Thread ID: " + Thread.currentThread().getId() + ", Processing Completed Id: " +
              message.getId() + ", Type: " + message.getType() + ", CaseId: " + message.getCaseId());
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      super.open(openContext);
    }
  }
}
