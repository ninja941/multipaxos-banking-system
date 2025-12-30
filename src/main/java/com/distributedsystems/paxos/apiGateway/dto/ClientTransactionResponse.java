package com.distributedsystems.paxos.apiGateway.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClientTransactionResponse {
    private boolean success;
    private String leaderId;
    private Long ballot;
    private Long sequence;
    private String message;

    public static ClientTransactionResponse check(String leaderId, Long ballot, Long seq, String msg) {
        ClientTransactionResponse r = new ClientTransactionResponse();
        r.success = true; r.leaderId = leaderId; r.ballot = ballot; r.sequence = seq; r.message = msg;
        return r;
    }
    public static ClientTransactionResponse fail(String leaderId, String msg) {
        ClientTransactionResponse r = new ClientTransactionResponse();
        r.success = false; r.leaderId = leaderId; r.message = msg;
        return r;
    }

    public boolean issuccess() { return success; }
    public String getLeaderId() { return leaderId; }
    public Long getBallot() { return ballot; }
    public Long getSequence() { return sequence; }
    public String getMessage() { return message; }
    public void setsuccess(boolean success) { this.success = success; }
    public void setLeaderId(String leaderId) { this.leaderId = leaderId; }
    public void setBallot(Long ballot) { this.ballot = ballot; }
    public void setSequence(Long sequence) { this.sequence = sequence; }
    public void setMessage(String message) { this.message = message; }
}
