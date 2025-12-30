package com.distributedsystems.paxos.service.scheduler;

import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.proto.Paxos.*;
import com.distributedsystems.paxos.service.phaseServiceImpl.RecoveryServiceImpl;
import com.distributedsystems.paxos.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HeartbeatServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatServiceImpl.class);

    private final NodeState nodeState;
    private final TransactionBlockRepository blocks;
    private final RecoveryServiceImpl catchup;

    public HeartbeatServiceImpl(NodeState ns,
                                TransactionBlockRepository blocks,
                                @Lazy RecoveryServiceImpl catchup) {
        this.nodeState = ns;
        this.blocks = blocks;
        this.catchup = catchup;
    }

    public HeartbeatReply onHeartbeat(HeartbeatRequest req) {
        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet()) {
            return HeartbeatReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                    .setBallot(Ballot.newBuilder().setNumber(nodeState.getHighestBallotSeen()))
                    .setLastCommitted(nodeState.getLastCommittedSeq())
                    .build();
        }

        final String sender = req.getFrom().getId();

        if (sender != null && !nodeState.isNodeLive(sender)) {
            log.debug("[{}] onHeartbeat: ignoring heartbeat from non-live {}", nodeState.getSelfNodeId(), sender);
            return HeartbeatReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                    .setBallot(Ballot.newBuilder().setNumber(nodeState.getHighestBallotSeen()))
                    .setLastCommitted(nodeState.getLastCommittedSeq())
                    .build();
        }

        long incomingBallot = req.getBallot().getNumber();

        if (incomingBallot < nodeState.getHighestBallotSeen()) {

            return HeartbeatReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                    .setBallot(Ballot.newBuilder().setNumber(nodeState.getHighestBallotSeen()))
                    .setLastCommitted(nodeState.getLastCommittedSeq())
                    .build();
        }


        if (incomingBallot > nodeState.getHighestBallotSeen()) {
            nodeState.updateHighestBallot(incomingBallot);
        }
        nodeState.setLeaderId(sender);
        nodeState.resetLeaderTraffic();

        long leaderTip = req.getLastCommitted();
        nodeState.setLeaderLastCommittedHint(leaderTip);

        if (!nodeState.isLeader()
                && leaderTip > nodeState.getLastCommittedSeq()
                && nodeState.isSelfLive()
                && !nodeState.isForceFailedForSet()) {
            try { catchup.pollOnce(); } catch (Exception ignore) {}
        }

        return HeartbeatReply.newBuilder()
                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setBallot(Ballot.newBuilder().setNumber(nodeState.getHighestBallotSeen()))
                .setLastCommitted(nodeState.getLastCommittedSeq())
                .build();
    }


    public SyncReply onSync(SyncRequest req) {
        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet()) {
            return SyncReply.newBuilder()
                    .setLeaderLastCommitted(0)
                    .build();
        }

        long fromSeq = Math.max(1, req.getFromSeq());
        long leaderTip = blocks.maxCommittedSeq();
        if (fromSeq > leaderTip) {
            return SyncReply.newBuilder()
                    .setLeaderLastCommitted(leaderTip)
                    .build();
        }

        List<TransactionBlock> rows = blocks.findRangeCommitted(fromSeq, leaderTip);

        SyncReply.Builder out = SyncReply.newBuilder()
                .setLeaderLastCommitted(leaderTip);

        for (TransactionBlock b : rows) {
            ClientRequest.Builder cr = ClientRequest.newBuilder()
                    .setClientId(b.getClientId() == null ? "" : b.getClientId())
                    .setTimestamp(b.getClientTsMillis());

            ClientRequestValue.Builder cv = ClientRequestValue.newBuilder();
            if ("noop-client".equals(b.getClientId())) {
                cv.setNoop(true);
            } else {
                cv.setTransfer(Transfer.newBuilder()
                        .setFrom(b.getFromAccountId())
                        .setTo(b.getToAccountId())
                        .setAmount(b.getAmountCents() == null ? 0L : b.getAmountCents()));
            }
            cr.setValue(cv);

            out.addCommitted(
                    AcceptEntry.newBuilder()
                            .setSequence(Sequence.newBuilder().setValue(b.getSequence()))
                            .setBallot(Ballot.newBuilder().setNumber(b.getBallot()))
                            .setRequest(cr)
                            .build()
            );
        }
        return out.build();
    }

}
