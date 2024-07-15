package org.appian.producer;

import com.fasterxml.jackson.databind.ObjectMapper;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.appian.common.Message;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerClient {

    private static final String TOPIC = "flinktopic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        Random random = new Random();

        try {
            for (int i = 0; i < 100; i++) {
                Message message = createRandomMessage(i, random);
                String messageJson = objectMapper.writeValueAsString(message);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, messageJson);

               // Thread.sleep(50);
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get();

                System.out.printf("Sent message: %s, Offset: %d, Partition: %d%n",
                        messageJson, metadata.offset(), metadata.partition());
            }
        } catch (InterruptedException | ExecutionException | com.fasterxml.jackson.core.JsonProcessingException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static Message createRandomMessage(int i, Random random) {
        String[] types = {"Contract", "eMessage", "Worksheet"};
        String type = types[random.nextInt(types.length)];
        String caseId = "case" + random.nextInt(2);

        Message message;
        switch (type) {
            case "Contract":
                message = new ContractMessage();
                break;
            case "eMessage":
                message = new eMessage();
                break;
            case "Worksheet":
                message = new WorksheetMessage();
                break;
            default:
                message = new Message();
        }
        message.setId(i);
        message.setType(type);
        message.setCaseId(caseId);
        message.setContent("Content for " + type + " with case ID " + caseId);

        return message;
    }
}
