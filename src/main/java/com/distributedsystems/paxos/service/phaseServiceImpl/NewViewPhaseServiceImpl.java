package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.proto.Paxos.Ballot;
import com.distributedsystems.paxos.proto.Paxos.NewViewReply;
import com.distributedsystems.paxos.proto.Paxos.NewViewRequest;
import com.distributedsystems.paxos.proto.Paxos.NodeId;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
public class NewViewPhaseServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(NewViewPhaseServiceImpl.class);

    private final NodeState nodeState;
    private final RecoveryServiceImpl recoveryService;

    public NewViewPhaseServiceImpl(NodeState nodeState, @Lazy RecoveryServiceImpl catchup) {
        this.nodeState = nodeState;
        this.recoveryService = catchup;
    }

    public void newView(NewViewRequest req, StreamObserver<NewViewReply> responseObserver) {
        try {
            NewViewReply reply = onNewView(req);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] Error handling NEW-VIEW: {}", nodeState.getSelfNodeId(), e.toString(), e);
            responseObserver.onError(e);
        }
    }

    public NewViewReply onNewView(NewViewRequest req) {
        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet()) {
            return NewViewReply.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                    .setBallot(Ballot.newBuilder()
                            .setNumber(nodeState.getHighestBallotSeen())
                            .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId())))
                    .setOk(false)
                    .setAcceptedCount(0)
                    .build();
        }

        long newBallot = req.getBallot().getNumber();
        nodeState.updateHighestBallot(newBallot);
        nodeState.setLeaderId(req.getFrom().getId());

        int count = (req.getEntriesCount() > 0)
                ? req.getEntriesCount()
                : req.getMergedLog().getEntriesCount();

        long leaderTip;
        if (req.getEntriesCount() > 0) {
            nodeState.replaceCommittedLog(req.getEntriesList());
            leaderTip = (req.getEntriesCount() == 0)
                    ? nodeState.getLastCommittedSeq()
                    : req.getEntries(req.getEntriesCount() - 1).getSequence().getValue();
            nodeState.setLeaderLastCommittedHint(leaderTip);
            log.debug("[{}] NEW-VIEW from {} ballot={} entries={} (leaderTip={})",
                    nodeState.getSelfNodeId(), req.getFrom().getId(), newBallot, count, leaderTip);
        } else {
            nodeState.replaceCommittedLog(req.getMergedLog().getEntriesList());
            leaderTip = (req.getMergedLog().getEntriesCount() == 0)
                    ? nodeState.getLastCommittedSeq()
                    : req.getMergedLog().getEntries(req.getMergedLog().getEntriesCount() - 1)
                    .getSequence().getValue();
            nodeState.setLeaderLastCommittedHint(leaderTip);
            log.debug("[{}] NEW-VIEW (merged) from {} ballot={} entries={} (leaderTip={})",
                    nodeState.getSelfNodeId(), req.getFrom().getId(), newBallot, count, leaderTip);
        }

        if (req.getEntriesCount() > 0) {
            nodeState.recordNewView(req.getFrom().getId(), newBallot, req.getEntriesList());
        } else {
            nodeState.recordNewView(req.getFrom().getId(), newBallot, req.getMergedLog().getEntriesList());
        }

        try { recoveryService.pollOnce(); } catch (Exception ignore) {}

        return NewViewReply.newBuilder()
                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setBallot(Ballot.newBuilder()
                        .setNumber(nodeState.getHighestBallotSeen())
                        .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId())))
                .setOk(true)
                .setAcceptedCount(count)
                .build();
    }
}
