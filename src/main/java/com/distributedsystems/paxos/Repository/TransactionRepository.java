package com.distributedsystems.paxos.Repository;

import com.distributedsystems.paxos.model.Transaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {
    List<Transaction> findByBlockId(Long blockId);

}
