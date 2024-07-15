package org.appian.common;

import lombok.Data;

@Data
public class Message {
    private int id;
    private String type;
    private String caseId;
    private String content;
}
