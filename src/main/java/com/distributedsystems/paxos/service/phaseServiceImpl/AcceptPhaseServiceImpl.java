package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.proto.Paxos;
import com.distributedsystems.paxos.proto.Paxos.AcceptEntry;
import com.distributedsystems.paxos.proto.Paxos.AcceptRequest;
import com.distributedsystems.paxos.proto.Paxos.AcceptedReply;
import com.distributedsystems.paxos.proto.Paxos.NodeId;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Service
public class AcceptPhaseServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(AcceptPhaseServiceImpl.class);

    private final NodeState ns;
    private final TransactionBlockRepository blockRepo;

    public AcceptPhaseServiceImpl(NodeState ns, TransactionBlockRepository blockRepo) {
        this.ns = ns;
        this.blockRepo = blockRepo;
    }

    @Transactional
    public AcceptedReply onAccept(AcceptRequest req) {
        AcceptEntry e = req.getEntry();
        long seq = e.getSequence().getValue();
        long incomingBallot = e.getBallot().getNumber();
        String fromNode = req.hasFrom() ? req.getFrom().getId() : "unknown";

        if (!ns.isSelfLive() || ns.isForceFailedForSet()) {
            log.info("[{}] DROP ACCEPT seq={} (node not live/forced-fail) from={}",
                    ns.getSelfNodeId(), seq, fromNode);
            return reply(false, e);
        }

        long promised = ns.getHighestBallotSeen();
        if (incomingBallot < promised) {
            log.info("[{}] ACCEPT NACK: stale ballot {} < promised {} (seq={}, from={})",
                    ns.getSelfNodeId(), incomingBallot, promised, seq, fromNode);

            Paxos.AcceptEntry preempt = Paxos.AcceptEntry.newBuilder(e)
                    .setBallot(Paxos.Ballot.newBuilder().setNumber(promised))
                    .build();
            return Paxos.AcceptedReply.newBuilder()
                    .setFrom(Paxos.NodeId.newBuilder().setId(ns.getSelfNodeId()))
                    .setEntry(preempt)
                    .setOk(false)
                    .build();
        }
        if (fromNode != null && !"unknown".equals(fromNode)) {
            ns.setLeaderId(fromNode);
            ns.resetLeaderTraffic();
        }


        TransactionBlock existing = blockRepo.findBySequence(seq).orElse(null);
        if (existing != null) {
            long acceptedBallot = existing.getBallot() == null ? Long.MIN_VALUE : existing.getBallot();

            // Lower than what we already accepted for THIS seq? NACK/ignore.
            if (incomingBallot < acceptedBallot) {
                log.info("[{}] ACCEPT NACK: ballot {} < acceptedBallot {} for seq={} (from={})",
                        ns.getSelfNodeId(), incomingBallot, acceptedBallot, seq, fromNode);
                return reply(false, e);
            }

            // Same ballot for same seq: accept idempotently only if value matches.
            if (incomingBallot == acceptedBallot) {
                if (sameValue(existing, e)) {
                    ns.updateHighestBallot(incomingBallot);
                    ns.appendAccepted(
                            seq,
                            existing.getFromAccountId(),
                            existing.getToAccountId(),
                            nz(existing.getAmountCents()),
                            acceptedBallot,
                            existing.getClientId(),
                            nz(existing.getClientTsMillis())
                    );

                    if (fromNode != null && !"unknown".equals(fromNode)) {
                        ns.setLeaderId(fromNode);
                        ns.resetLeaderTraffic();
                    }

                    log.debug("[{}] ACCEPT idempotent: seq={} ballot={} (from={})",
                            ns.getSelfNodeId(), seq, incomingBallot, fromNode);
                    return reply(true, e);
                } else {
                    // Same ballot, different value ⇒ protocol violation (two leaders).
                    log.warn("[{}] ACCEPT NACK: conflicting value at same ballot for seq={} ballot={} (from={})",
                            ns.getSelfNodeId(), seq, incomingBallot, fromNode);
                    return reply(false, e);
                }
            }
        }

        // 3) Accept the value for this seq and raise our promise
        ns.updateHighestBallot(incomingBallot);

        Paxos.ClientRequest cr = e.getRequest();
        TransactionBlock block = (existing != null) ? existing : new TransactionBlock();

        block.setSequence(seq);
        block.setBallot(incomingBallot);
        if (cr != null) {
            block.setClientId(cr.getClientId());
            block.setClientTsMillis(cr.getTimestamp());
        }

        if (cr != null && cr.hasValue() && cr.getValue().hasTransfer()) {
            Paxos.Transfer t = cr.getValue().getTransfer();
            block.setFromAccountId(t.getFrom());
            block.setToAccountId(t.getTo());
            block.setAmountCents(t.getAmount());
        } else {
            // Explicit NO-OP
            block.setClientId("noop-client");
            block.setFromAccountId(null);
            block.setToAccountId(null);
            block.setAmountCents(0L);
        }

        blockRepo.save(block);

        ns.appendAccepted(
                seq,
                block.getFromAccountId(),
                block.getToAccountId(),
                nz(block.getAmountCents()),
                incomingBallot,
                block.getClientId(),
                nz(block.getClientTsMillis())
        );

        if (fromNode != null && !"unknown".equals(fromNode)) {
            ns.setLeaderId(fromNode);
            ns.resetLeaderTraffic();
        }

        log.info("[{}] ACCEPT check ballot={} seq={} from={}",
                ns.getSelfNodeId(), incomingBallot, seq, fromNode);

        return reply(true, e);
    }

    public void accept(AcceptRequest req, StreamObserver<AcceptedReply> responseObserver) {
        try {
            responseObserver.onNext(onAccept(req));
            responseObserver.onCompleted();
        } catch (Exception ex) {
            log.error("[{}] Error in ACCEPT handler (seq={}): {}",
                    ns.getSelfNodeId(),
                    req.getEntry().getSequence().getValue(),
                    ex.toString(), ex);
            responseObserver.onError(ex);
        }
    }



    private AcceptedReply reply(boolean ok, AcceptEntry e) {
        return AcceptedReply.newBuilder()
                .setFrom(NodeId.newBuilder().setId(ns.getSelfNodeId()))
                .setEntry(e)
                .setOk(ok)
                .build();
    }

    private boolean sameValue(TransactionBlock b, AcceptEntry e) {
        Paxos.ClientRequest cr = e.getRequest();
        if (cr == null) {
            return (nz(b.getAmountCents()) == 0L) && b.getFromAccountId() == null && b.getToAccountId() == null;
        }
        if (!(cr.hasValue() && cr.getValue().hasTransfer())) {
            return (nz(b.getAmountCents()) == 0L) && b.getFromAccountId() == null && b.getToAccountId() == null;
        }
        Paxos.Transfer t = cr.getValue().getTransfer();
        return nz(b.getAmountCents()) == t.getAmount()
                && Objects.equals(b.getFromAccountId(), t.getFrom())
                && Objects.equals(b.getToAccountId(), t.getTo());
    }

    private long nz(Long v) { return v == null ? 0L : v; }
}
