package cn.mqtty.broker.msg;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActionMsg {
    String id;
    String sn;
    String type;
    JsonNode msg;
    Long timestamp;
}
