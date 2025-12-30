package com.distributedsystems.paxos.service.scheduler;

import com.distributedsystems.paxos.proto.Paxos;
import com.distributedsystems.paxos.proto.PaxosServiceGrpc;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class LeaderHeartbeatSchedulerImpl {

    private static final Logger log = LoggerFactory.getLogger(LeaderHeartbeatSchedulerImpl.class);

    private final NodeState nodeState;

    @Value("${paxos.heartbeat.scheduler.enabled:false}")
    private boolean enabled;

    @Value("${paxos.heartbeat.ms:400}")
    private long periodTme;

    private final long rpcDeadlineTime = 800;
    private final int  missQuorumThreshold = 20;
    private int consecutiveMissedQuorum = 0;

    public LeaderHeartbeatSchedulerImpl(NodeState ns) {
        this.nodeState = ns;
    }

    @Scheduled(fixedRateString = "${paxos.heartbeat.ms:400}", initialDelay = 400)
    public void sendHeartbeat() {
        if (!enabled) {
            return;
        }
        if (!nodeState.isLeader()) {
            consecutiveMissedQuorum = 0;
            return;
        }

        final long myBallot = nodeState.getHighestBallotSeen();

        Paxos.HeartbeatRequest hb = Paxos.HeartbeatRequest.newBuilder()
                .setFrom(Paxos.NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setBallot(Paxos.Ballot.newBuilder().setNumber(myBallot))
                .setLastCommitted(nodeState.getLastCommittedSeq())
                .build();

        log.info("[{}] HB out: lastCommitted={} periodTme={}",
                nodeState.getSelfNodeId(), nodeState.getLastCommittedSeq(), periodTme);

        int acks = 1;
        boolean sawHigherBallot = false;

        for (String peer : nodeState.getLiveNodes()) {
            if (peer.equals(nodeState.getSelfNodeId())) continue;

            HostPort hp = parse(peer);
            ManagedChannel ch = null;
            try {
                ch = ManagedChannelBuilder.forAddress(hp.host(), hp.port()).usePlaintext().build();
                PaxosServiceGrpc.PaxosServiceBlockingStub stub =
                        PaxosServiceGrpc.newBlockingStub(ch).withDeadlineAfter(rpcDeadlineTime, TimeUnit.MILLISECONDS);

                Paxos.HeartbeatReply r = stub.heartbeat(hb);
                long peerBallot = r.getBallot().getNumber();
//                if (peerBallot > myBallot) {
//                    sawHigherBallot = true;
//                } else if (peerBallot == myBallot) {
//                    acks++;
//                }
                if (peerBallot > myBallot) {
                    sawHigherBallot = true;
                } else {
                    acks++;
                }
            } catch (Exception ignore) {
            } finally {
                if (ch != null) ch.shutdownNow();
            }
        }

        if (sawHigherBallot) {
            nodeState.stepDown("heartbeat saw higher ballot");
            consecutiveMissedQuorum = 0;
            return;
        }

        boolean hasQuorum = acks >= quorumSize(nodeState.getLiveNodes().size());
        if (!hasQuorum) {
            consecutiveMissedQuorum++;
            if (consecutiveMissedQuorum >= missQuorumThreshold) {
                nodeState.stepDown("missed heartbeat quorum " + consecutiveMissedQuorum + "×");
                consecutiveMissedQuorum = 0;
            }
        } else {
            consecutiveMissedQuorum = 0;
        }
    }

    private static int quorumSize(int n) { return (Math.max(1, n) / 2) + 1; }

    private static HostPort parse(String target) {
        String[] parts = target.split(":");
        if (parts.length != 2) throw new IllegalArgumentException("Bad peer: " + target);
        return new HostPort(parts[0], Integer.parseInt(parts[1]));
    }
    private record HostPort(String host, int port) {}
}
