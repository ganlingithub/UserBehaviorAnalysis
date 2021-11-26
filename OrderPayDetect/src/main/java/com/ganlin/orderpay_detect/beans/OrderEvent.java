package com.ganlin.orderpay_detect.beans;

public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txid;
    private Long timestamp;

    public OrderEvent() {
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txid='" + txid + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxid() {
        return txid;
    }

    public void setTxid(String txid) {
        this.txid = txid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public OrderEvent(Long orderId, String eventType, String txid, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txid = txid;
        this.timestamp = timestamp;
    }
}
