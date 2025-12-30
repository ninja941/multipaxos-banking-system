package com.distributedsystems.paxos.model;

import jakarta.persistence.*;

@Entity
@Table(name = "customer_account")
public class CustomerAccount {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 128)
    private String name;

    @Column(nullable = false)
    private Long balance;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Long getBalance() { return balance; }
    public void setBalance(Long balance) { this.balance = balance; }

    public static class Builder {
        private String name;
        private Long balance;

        public Builder name(String n) { this.name = n; return this; }

        public CustomerAccount build() {
            CustomerAccount ca = new CustomerAccount();
            ca.setName(name);
            ca.setBalance(balance);
            return ca;
        }
    }

    public static Builder builder() { return new Builder(); }
}
