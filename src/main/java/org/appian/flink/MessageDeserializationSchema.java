package org.appian.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import java.io.IOException;

public class MessageDeserializationSchema implements DeserializationSchema<Message> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Message deserialize(byte[] messageBytes) throws IOException {
        return objectMapper.readValue(messageBytes, Message.class);
    }

    @Override
    public boolean isEndOfStream(Message nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Message> getProducedType() {
        return Types.POJO(Message.class);
    }
}
