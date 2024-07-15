package org.appian.flink;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message {

    private int id;
    private String type;
    private String caseId;
    private String content;
}






