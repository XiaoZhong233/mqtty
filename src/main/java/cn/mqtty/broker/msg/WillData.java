package cn.mqtty.broker.msg;

public class WillData {

    private String topic;
    private byte[] payload;
    private int qos;
    private boolean retain;

    public WillData(String topic, byte[] payload, int qos, boolean retain) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.retain = retain;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public int getQos() {
        return qos;
    }

    public boolean isRetain() {
        return retain;
    }
}
