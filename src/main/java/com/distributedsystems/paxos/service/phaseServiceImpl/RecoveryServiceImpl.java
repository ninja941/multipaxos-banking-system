package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.proto.Paxos;
import com.distributedsystems.paxos.proto.PaxosServiceGrpc;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class RecoveryServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(RecoveryServiceImpl.class);

    private final NodeState nodeState;
    private final CommitPhaseServiceImpl commitPhaseService;

    @Value("${paxos.catchup.rpcTimeoutMillis:1200}")
    private long rpcTimeoutMillis;

    @Value("${paxos.catchup.periodMillis:1500}")
    private long pollPeriodMillis;

    public RecoveryServiceImpl(NodeState ns, CommitPhaseServiceImpl commitSvc) {
        this.nodeState = ns;
        this.commitPhaseService = commitSvc;
    }

    @Scheduled(fixedDelayString = "${paxos.catchup.periodMillis:1500}", initialDelay = 1000)
    public void periodic() {
        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet() || nodeState.isLeader()) return;
        try {
            pollOnce();
        } catch (Exception e) {
        }
    }

    public void pollOnce() {
        if (!nodeState.isSelfLive() || nodeState.isForceFailedForSet() || nodeState.isLeader()) return;

        final long local = nodeState.getLastCommittedSeq();

        LinkedHashSet<String> targets = new LinkedHashSet<>();
        String leader = nodeState.getLeaderId();
        if (leader != null) targets.add(leader);
        for (String p : nodeState.livePeers()) targets.add(p);

        targets.remove(nodeState.getSelfNodeId());
        if (targets.isEmpty()) return;

        long maxLeaderTipSeen = local;
        int applied = 0;

        for (String peer : targets) {
            ManagedChannel ch = null;
            try {
                ch = ManagedChannelBuilder.forTarget(peer).usePlaintext().build();
                PaxosServiceGrpc.PaxosServiceBlockingStub stub =
                        PaxosServiceGrpc.newBlockingStub(ch).withDeadlineAfter(rpcTimeoutMillis, TimeUnit.MILLISECONDS);

                Paxos.SyncReply sr = stub.sync(
                        Paxos.SyncRequest.newBuilder()
                                .setFrom(Paxos.NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                .setFromSeq(local + 1)
                                .build());

                long leaderTip = sr.getLeaderLastCommitted();
                if (leaderTip > maxLeaderTipSeen) maxLeaderTipSeen = leaderTip;

                if (sr.getCommittedCount() == 0) {
                    continue;
                }

                for (Paxos.AcceptEntry e : sr.getCommittedList()) {
                    try {
                        commitPhaseService.onCommit(
                                Paxos.CommitRequest.newBuilder()
                                        .setFrom(Paxos.NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                        .setEntry(e)
                                        .build());
                        applied++;
                    } catch (Exception applyErr) {
                        log.debug("[{}] catch-up apply failed at seq={} from {}: {}",
                                nodeState.getSelfNodeId(), e.getSequence().getValue(), peer, applyErr.toString());
                    }
                }

                break;

            } catch (StatusRuntimeException ex) {
                log.debug("[{}] catch-up probe to {} failed: {}", nodeState.getSelfNodeId(), peer, ex.getStatus());
            } catch (Exception ex) {
                log.debug("[{}] catch-up probe to {} error: {}", nodeState.getSelfNodeId(), peer, ex.toString());
            } finally {
                if (ch != null) ch.shutdownNow();
            }
        }

        if (applied > 0 || maxLeaderTipSeen > local) {
            log.info("[{}] catch-up applied {} entries (local {} → {}), leaderTipHint={}",
                    nodeState.getSelfNodeId(),
                    applied,
                    local,
                    nodeState.getLastCommittedSeq(),
                    Math.max(maxLeaderTipSeen, nodeState.getLeaderLastCommittedHint()));
            nodeState.setLeaderLastCommittedHint(Math.max(maxLeaderTipSeen, nodeState.getLeaderLastCommittedHint()));
        }
    }
}
