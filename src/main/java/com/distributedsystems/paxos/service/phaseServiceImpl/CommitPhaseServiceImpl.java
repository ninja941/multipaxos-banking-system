package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.Repository.CustomerAccountRepository;
import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.Repository.TransactionRepository;
import com.distributedsystems.paxos.model.CustomerAccount;
import com.distributedsystems.paxos.model.Transaction;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.proto.Paxos.AcceptEntry;
import com.distributedsystems.paxos.proto.Paxos.ClientRequest;
import com.distributedsystems.paxos.proto.Paxos.CommitReply;
import com.distributedsystems.paxos.proto.Paxos.CommitRequest;
import com.distributedsystems.paxos.proto.Paxos.NodeId;
import com.distributedsystems.paxos.proto.Paxos.Transfer;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

@Service
public class CommitPhaseServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(CommitPhaseServiceImpl.class);

    private final NodeState nodeState;
    private final TransactionBlockRepository transactionBlockRepository;
    private final TransactionRepository transactionRepository;
    private final CustomerAccountRepository customerAccountRepository;

    public CommitPhaseServiceImpl(NodeState ns,
                                  TransactionBlockRepository transactionBlockRepository,
                                  TransactionRepository transactionRepository,
                                  CustomerAccountRepository customerAccountRepository) {
        this.nodeState = ns;
        this.transactionBlockRepository = transactionBlockRepository;
        this.transactionRepository = transactionRepository;
        this.customerAccountRepository = customerAccountRepository;
    }

    @Transactional
    public CommitReply onCommit(CommitRequest req) {
        if (!nodeState.isSelfLive()) {
            log.info("[{}] DROP COMMIT seq={} (node not live for this set)",
                    nodeState.getSelfNodeId(), req.getEntry().getSequence().getValue());
            throw io.grpc.Status.UNAVAILABLE
                    .withDescription("node not live for this set").asRuntimeException();
        }
        final AcceptEntry entry = req.getEntry();
        final long seq = entry.getSequence().getValue();
        final long nowMillis = System.currentTimeMillis();

        log.info("[{}] onCommit(seq={}) ENTER", nodeState.getSelfNodeId(), seq);

        if (transactionBlockRepository.existsBySequence(seq)) {
            TransactionBlock existing = transactionBlockRepository.findBySequence(seq).orElse(null);
            if (existing != null && existing.getExecutedAt() != null) {
                log.info("[{}] onCommit(seq={}) already applied (block#{}); skipping re-execution",
                        nodeState.getSelfNodeId(), seq, existing.getId());
                nodeState.markCommitted(seq, existing);
                nodeState.executeReady();
                return reply(seq);
            }
        }




        if (req.hasFrom()) {
            nodeState.setLeaderId(req.getFrom().getId());
            nodeState.resetLeaderTraffic();
        }

        final ClientRequest cr = entry.getRequest();
        final String clientId = cr.getClientId();
        final long clientTs = cr.getTimestamp();

        final boolean hasValue = cr.hasValue();
        final boolean hasTransfer = hasValue && cr.getValue().hasTransfer();
        final Transfer transfer = hasTransfer ? cr.getValue().getTransfer() : null;

        TransactionBlock block = transactionBlockRepository.findBySequenceForUpdate(seq).orElse(null);

        Optional<TransactionBlock> byClientKey = transactionBlockRepository.findByClientIdAndClientTsMillis(clientId, clientTs);
        if (byClientKey.isPresent() && !byClientKey.get().getSequence().equals(seq)) {
            TransactionBlock noop = (block != null) ? block : new TransactionBlock();
            noop.setSequence(seq);
            noop.setBallot(entry.getBallot().getNumber());
            noop.setClientId("noop-client");
            noop.setClientTsMillis(nowMillis);
            noop.setFromAccountId(null);
            noop.setToAccountId(null);
            noop.setAmountCents(0L);
            if (noop.getCommittedAt() == null) noop.setCommittedAt(Instant.ofEpochMilli(nowMillis));
            if (noop.getExecutedAt() == null) noop.setExecutedAt(Instant.ofEpochMilli(nowMillis));

            noop = saveOrReloadOnUniqueSeq(noop, seq);

            log.info("[{}] onCommit(seq={}) DUPLICATE of seq={}; wrote NO-OP block#{}",
                    nodeState.getSelfNodeId(), seq, byClientKey.get().getSequence(), noop.getId());

            nodeState.markCommitted(seq, noop);
            nodeState.executeReady();
            return reply(seq);
        }

        if (block == null) {
            block = new TransactionBlock();
            block.setSequence(seq);
        }
        block.setBallot(entry.getBallot().getNumber());
        block.setClientId(clientId);
        block.setClientTsMillis(clientTs);
        if (block.getCommittedAt() == null) {
            block.setCommittedAt(Instant.ofEpochMilli(nowMillis));
        }

        final boolean isNoopClient = "noop-client".equals(clientId);
        boolean treatAsNoop = isNoopClient || !hasTransfer;
        if (hasTransfer && transfer.getAmount() <= 0) {
            log.warn("[{}] onCommit(seq={}) non-positive amount {}; treating as NO-OP",
                    nodeState.getSelfNodeId(), seq, transfer.getAmount());
            treatAsNoop = true;
        }

        if (block.getExecutedAt() != null) {
            log.info("[{}] onCommit(seq={}) already executed (block#{}), idempotent return",
                    nodeState.getSelfNodeId(), seq, safeId(block));
            nodeState.markCommitted(seq, block);
            nodeState.executeReady();
            return reply(seq);
        }

        if (treatAsNoop) {
            block.setFromAccountId(null);
            block.setToAccountId(null);
            block.setAmountCents(0L);
            block.setExecutedAt(Instant.ofEpochMilli(nowMillis));
            block = saveOrReloadOnUniqueSeq(block, seq);

            log.info("[{}] onCommit(seq={}) NO-OP committed and executed", nodeState.getSelfNodeId(), seq);
            nodeState.markCommitted(seq, block);
            nodeState.executeReady();
            return reply(seq);
        }

        final long amt = transfer.getAmount();
        block.setFromAccountId(transfer.getFrom());
        block.setToAccountId(transfer.getTo());
        block.setAmountCents(amt);

        CustomerAccount sender = customerAccountRepository.findByName(transfer.getFrom()).orElseGet(() -> {
            CustomerAccount acc = new CustomerAccount();
            acc.setName(transfer.getFrom());
            acc.setBalance(0L);
            return customerAccountRepository.save(acc);
        });
        CustomerAccount receiver = customerAccountRepository.findByName(transfer.getTo()).orElseGet(() -> {
            CustomerAccount acc = new CustomerAccount();
            acc.setName(transfer.getTo());
            acc.setBalance(0L);
            return customerAccountRepository.save(acc);
        });

        if (sender.getBalance() < amt) {
            block.setExecutedAt(Instant.ofEpochMilli(nowMillis));
            block = saveOrReloadOnUniqueSeq(block, seq);

            log.info("[{}] onCommit(seq={}) INSUFFICIENT FUNDS: {} has {}, needs {}. Recorded failed execution.",
                    nodeState.getSelfNodeId(), seq, transfer.getFrom(), sender.getBalance(), amt);

            nodeState.markCommitted(seq, block);
            nodeState.executeReady();
            return reply(seq);
        }

        block = saveOrReloadOnUniqueSeq(block, seq);

        Transaction tx = new Transaction();
        tx.setSenderId(sender.getId());
        tx.setReceiverId(receiver.getId());
        tx.setAmount(amt);
        tx.setTimestamp(LocalDateTime.ofInstant(Instant.ofEpochMilli(clientTs), ZoneId.systemDefault()));
        tx.setBlock(block);
        transactionRepository.save(tx);

        sender.setBalance(sender.getBalance() - amt);
        receiver.setBalance(receiver.getBalance() + amt);
        customerAccountRepository.save(sender);
        customerAccountRepository.save(receiver);

        block.setExecutedAt(Instant.ofEpochMilli(nowMillis));
        transactionBlockRepository.save(block);

        log.info("[{}] onCommit(seq={}) APPLIED {} -> {} : {} (senderBal={}, receiverBal={})",
                nodeState.getSelfNodeId(), seq, transfer.getFrom(), transfer.getTo(), amt,
                sender.getBalance(), receiver.getBalance());

        nodeState.markCommitted(seq, block);
        nodeState.executeReady();

        log.info("[{}] onCommit(seq={}) EXIT", nodeState.getSelfNodeId(), seq);

        return reply(seq);
    }

    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        log.info("[{}] commit() RPC received for seq={} from {}",
                nodeState.getSelfNodeId(),
                request.getEntry().getSequence().getValue(),
                request.getFrom().getId());
        try {
            CommitReply reply = onCommit(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] Commit failed (seq={})",
                    nodeState.getSelfNodeId(),
                    request.getEntry().getSequence().getValue(), e);
            responseObserver.onError(e);
        }
    }

    private CommitReply reply(long seq) {
        return CommitReply.newBuilder()
                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setCommittedSeq(seq)
                .build();
    }

    private TransactionBlock saveOrReloadOnUniqueSeq(TransactionBlock tb, long seq) {
        try {
            return transactionBlockRepository.save(tb);
        } catch (DataIntegrityViolationException dup) {
            return transactionBlockRepository.findBySequenceForUpdate(seq).orElseThrow(() -> dup);
        }
    }

    private static Object safeId(TransactionBlock b) {
        return (b == null) ? "null" : b.getId();
    }
}
