package com.distributedsystems.paxos.model;

import jakarta.persistence.*;
import java.time.Instant;
import jakarta.persistence.*;
import org.hibernate.annotations.DynamicUpdate;

import java.util.Objects;

@Entity
@DynamicUpdate
@Table(
        name = "transaction_block",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_sequence", columnNames = "sequence"),
                @UniqueConstraint(name = "uk_client_op", columnNames = {"client_id", "client_ts_millis"})
        },
        indexes = {
                @Index(name = "idx_tb_sequence", columnList = "sequence"),
                @Index(name = "idx_tb_client_op", columnList = "client_id, client_ts_millis")
        }
)
public class TransactionBlock {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, updatable = false)
    private Long sequence;

    @Column(name = "client_id", nullable = false, length = 128, updatable = false)
    private String clientId;

    @Column(name = "client_ts_millis", nullable = false, updatable = false)
    private Long clientTsMillis;

    @Column(nullable = false/*, updatable = false*/)
    private Long ballot;

    @Column(name = "amount_cents", updatable = false)
    private Long amountCents;

    @Column(name = "from_account_id", length = 64, updatable = false)
    private String fromAccountId;

    @Column(name = "to_account_id", length = 64, updatable = false)
    private String toAccountId;

    @Column(name = "committed_at", nullable = true)
    private Instant committedAt;

    @Column(name = "executed_at", nullable = true)
    private Instant executedAt;

    @Version
    @Column(name = "version")
    private Long version;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getSequence() { return sequence; }
    public void setSequence(Long sequence) { this.sequence = sequence; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public Long getClientTsMillis() { return clientTsMillis; }
    public void setClientTsMillis(Long clientTsMillis) { this.clientTsMillis = clientTsMillis; }

    public Long getBallot() { return ballot; }
    public void setBallot(Long ballot) { this.ballot = ballot; }

    public Long getAmountCents() { return amountCents; }
    public void setAmountCents(Long amountCents) { this.amountCents = amountCents; }

    public String getFromAccountId() { return fromAccountId; }
    public void setFromAccountId(String fromAccountId) { this.fromAccountId = fromAccountId; }

    public String getToAccountId() { return toAccountId; }
    public void setToAccountId(String toAccountId) { this.toAccountId = toAccountId; }

    public Instant getCommittedAt() { return committedAt; }
    public void setCommittedAt(Instant committedAt) { this.committedAt = committedAt; }

    public Instant getExecutedAt() { return executedAt; }
    public void setExecutedAt(Instant executedAt) { this.executedAt = executedAt; }

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }

    public boolean isCommitted() { return committedAt != null; }
    public boolean isExecuted() { return executedAt != null; }

    public void markCommitted(Instant when) {
        if (this.committedAt == null) this.committedAt = when;
    }

    public void markExecuted(Instant when) {
        if (this.executedAt == null) this.executedAt = when;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionBlock that)) return false;
        return Objects.equals(sequence, that.sequence);
    }
    @Override public int hashCode() { return Objects.hash(sequence); }

    public static class Builder {
        private Long sequence;
        private String clientId;
        private Long clientTsMillis;
        private Long ballot;
        private Long amountCents;
        private String fromAccountId;
        private String toAccountId;
        private Instant committedAt;
        private Instant executedAt;

        public Builder sequence(Long seq) { this.sequence = seq; return this; }
        public Builder clientId(String c) { this.clientId = c; return this; }
        public Builder clientTsMillis(Long ts) { this.clientTsMillis = ts; return this; }
        public Builder ballot(Long b) { this.ballot = b; return this; }
        public Builder amountCents(Long a) { this.amountCents = a; return this; }
        public Builder fromAccountId(String f) { this.fromAccountId = f; return this; }
        public Builder toAccountId(String t) { this.toAccountId = t; return this; }


        public TransactionBlock build() {
            TransactionBlock tb = new TransactionBlock();
            tb.setSequence(sequence);
            tb.setClientId(clientId);
            tb.setClientTsMillis(clientTsMillis);
            tb.setBallot(ballot);
            tb.setAmountCents(amountCents);
            tb.setFromAccountId(fromAccountId);
            tb.setToAccountId(toAccountId);
            tb.setCommittedAt(committedAt);
            tb.setExecutedAt(executedAt);
            return tb;
        }
    }

    public static Builder builder() { return new Builder(); }
}
