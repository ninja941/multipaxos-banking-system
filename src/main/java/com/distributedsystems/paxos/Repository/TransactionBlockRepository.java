package com.distributedsystems.paxos.Repository;

import com.distributedsystems.paxos.model.TransactionBlock;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface TransactionBlockRepository extends JpaRepository<TransactionBlock, Long> {

    Optional<TransactionBlock> findBySequence(Long sequence);

    Optional<TransactionBlock> findByClientIdAndClientTsMillis(String clientId, Long clientTsMillis);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select t from TransactionBlock t where t.sequence = :seq")
    Optional<TransactionBlock> findBySequenceForUpdate(@Param("seq") Long seq);

    List<TransactionBlock> findAllByOrderBySequenceAsc();

    @Query("""
           select b from TransactionBlock b
           where b.committedAt is not null
             and b.sequence between :from and :to
           order by b.sequence asc
           """)
    List<TransactionBlock> findRangeCommitted(@Param("from") Long from,
                                              @Param("to") Long to);

    @Query("""
           select t from TransactionBlock t
           where t.committedAt is not null and t.executedAt is null
           order by t.sequence asc
           """)
    List<TransactionBlock> findCommittedNotExecuted();

    @Query("select coalesce(max(b.sequence), 0) from TransactionBlock b where b.committedAt is not null")
    long maxCommittedSeq();

    @Query("select coalesce(max(b.ballot), 0) from TransactionBlock b")
    long maxBallot();

    @Query("select coalesce(max(tb.sequence), 0) from TransactionBlock tb")
    long findMaxSequence();

    boolean existsBySequence(long sequence);
}
