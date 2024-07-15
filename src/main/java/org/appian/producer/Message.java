package com.appian.flink.producer;

import lombok.Data;

@Data
public class Message {
    private int id;
    private String type;
    private String caseId;
    private String content;
}
