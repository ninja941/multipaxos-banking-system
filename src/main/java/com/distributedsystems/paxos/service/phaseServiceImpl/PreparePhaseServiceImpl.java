package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.proto.Paxos.*;
import com.distributedsystems.paxos.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class PreparePhaseServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(PreparePhaseServiceImpl.class);

    private final NodeState nodeState;
    private final TransactionBlockRepository transactionBlockRepository;

    @Value("${paxos.followers.timer.ms:1500}")
    private long followerTimerMs;

    public PreparePhaseServiceImpl(NodeState ns, TransactionBlockRepository blockRepo) {
        this.nodeState = ns;
        this.transactionBlockRepository = blockRepo;
    }

    public PromiseReply onPrepare(PrepareRequest req) {
        final String self = nodeState.getSelfNodeId();
        final long incoming = req.getBallot().getNumber();
        final long highest = nodeState.getHighestBallotSeen();

        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet()) {
            return PromiseReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(self))
                    .setBallot(Ballot.newBuilder().setNumber(highest))
                    .setTimerExpired(false)
                    .build();
        }

        boolean timerExpired = nodeState.isFollowerTimerExpired(followerTimerMs);

        if (!timerExpired || incoming <= highest) {
            log.debug("[{}] PREPARE defer: expired={}, incoming={}, highest={}",
                    self, timerExpired, incoming, highest);

            return PromiseReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(self))
                    .setBallot(Ballot.newBuilder().setNumber(highest))
                    .setTimerExpired(false)
                    .build();
        }

        nodeState.updateHighestBallot(incoming);
        nodeState.markPrepareSeenNow();

        List<TransactionBlock> blocks = transactionBlockRepository.findAllByOrderBySequenceAsc();

        List<AcceptEntry> accepted = blocks.stream()
                .sorted(Comparator.comparingLong(TransactionBlock::getSequence))
                .map(b -> {
                    ClientRequest.Builder cr = ClientRequest.newBuilder()
                            .setClientId(b.getClientId() == null ? "" : b.getClientId())
                            .setTimestamp(b.getClientTsMillis());

                    ClientRequestValue.Builder val = ClientRequestValue.newBuilder();
                    if ("noop-client".equals(b.getClientId())) {
                        val.setNoop(true);
                    } else {
                        val.setTransfer(Transfer.newBuilder()
                                .setFrom(b.getFromAccountId())
                                .setTo(b.getToAccountId())
                                .setAmount(b.getAmountCents() == null ? 0L : b.getAmountCents()));
                    }
                    cr.setValue(val);

                    return AcceptEntry.newBuilder()
                            .setBallot(Ballot.newBuilder().setNumber(b.getBallot()))
                            .setSequence(Sequence.newBuilder().setValue(b.getSequence()))
                            .setRequest(cr)
                            .build();
                })
                .collect(Collectors.toList());

        log.info("[{}] PREPARE promise: incomingBallot={}, acceptLogSize={}",
                self, incoming, accepted.size());

        return PromiseReply.newBuilder()
                .setFrom(NodeId.newBuilder().setId(self))
                .setBallot(Ballot.newBuilder().setNumber(incoming))
                .setTimerExpired(true)
                .addAllAccepted(accepted)
                .build();
    }
}
