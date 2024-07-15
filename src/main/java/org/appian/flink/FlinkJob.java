package org.appian.flink;

import java.time.Instant;

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

public class FlinkJob {

  public void start() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("flinktopic")
        .setGroupId("flink-group")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),
        "Kafka Source");

    kafkaStream
        .map(new JsonToEventMapFunction())
        .keyBy(m -> m.getType()+"-"+m.getCaseId())
        //.window(TumblingProcessingTimeWindows.of(Duration.of(5, ChronoUnit.SECONDS)))
        .process(new MessageRouterProcessFunction())
        .setParallelism(3);


    env.execute("Executing stream processor");
  }

  private static class MessageRouterProcessFunction extends KeyedProcessFunction<String, Message, Void> {

    @Override
    public void processElement(
        Message message, KeyedProcessFunction<String, Message, Void>.Context context, Collector<Void> collector)
        throws Exception {
//      switch (message.getType()) {
//        case "Contract":
//          System.out.println("Processing Contract");
//          break;
//        case "eMessage":
//          System.out.println("Processing eMessage");
//          break;
//        case "Worksheet":
//          System.out.println("Processing Worksheet");
//          break;
//        default:
//          throw new IllegalArgumentException("Unknown event type: " + message.getType());
//      }
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


