package com.ganlin.orderpay_detect.beans;

public class ReceiptEvent {
    private String txId;
    private String channel;
    private Long timestamp;

    @Override
    public String toString() {
        return "ReciptEvent{" +
                "txId='" + txId + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public ReceiptEvent(String txId, String channel, Long timestamp) {
        this.txId = txId;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public ReceiptEvent() {
    }
}
