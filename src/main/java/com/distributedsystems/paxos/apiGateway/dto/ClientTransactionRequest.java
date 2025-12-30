package com.distributedsystems.paxos.apiGateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientTransactionRequest {
    private String from;
    private String to;
    private long amount;

    private String clientId;
    private Long clientTsMillis;

    public ClientTransactionRequest() {}
    public ClientTransactionRequest(String from, String to, long amount) {
        this.from = from; this.to = to; this.amount = amount;
    }

    public String getFrom() { return from; }
    public void setFrom(String from) { this.from = from; }
    public String getTo() { return to; }
    public void setTo(String to) { this.to = to; }
    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }
    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public Long getClientTsMillis() { return clientTsMillis; }
    public void setClientTsMillis(Long clientTsMillis) { this.clientTsMillis = clientTsMillis; }
}
