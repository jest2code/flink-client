package org.appian.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.appian.common.Message;

import java.time.Instant;

public class FlinkJobSideOutput {

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

    final OutputTag<Message> contractTag = new OutputTag<Message>("Contract"){};
    final OutputTag<Message> eMessageTag = new OutputTag<Message>("eMessage"){};
    final OutputTag<Message> worksheetTag = new OutputTag<Message>("Worksheet"){};

    DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");


    SingleOutputStreamOperator<Message> mainDataStream = kafkaStream
        .map(new JsonToEventMapFunction())
        .process(new ProcessFunction<Message, Message>() {
          @Override
          public void processElement(Message message, Context context, Collector<Message> collector) throws Exception {
            switch (message.getType()) {
              case "Contract":
                context.output(contractTag, message);
                break;
              case "eMessage":
                context.output(eMessageTag, message);
                break;
              case "Worksheet":
                context.output(worksheetTag, message);
                break;
              default:
                throw new IllegalArgumentException("Unknown event type: " + message.getType());
            }
          }
        });

    DataStream<Message> contractStream = mainDataStream.getSideOutput(contractTag);
    DataStream<Message> eMessageStream = mainDataStream.getSideOutput(eMessageTag);
    DataStream<Message> worksheetStream = mainDataStream.getSideOutput(worksheetTag);

    contractStream
        .keyBy(Message::getCaseId)
        .process(new MessageRouterProcessFunction())
        .setParallelism(4);

    eMessageStream
        .keyBy(Message::getCaseId)
        .process(new MessageRouterProcessFunction())
        .setParallelism(4);

    worksheetStream
        .keyBy(Message::getCaseId)
        .process(new MessageRouterProcessFunction())
        .setParallelism(4);

    env.execute("Executing stream processor with side output");
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
