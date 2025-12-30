package com.distributedsystems.paxos.service.phaseServiceImpl;

import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.proto.Paxos.*;
import com.distributedsystems.paxos.proto.PaxosServiceGrpc;
import com.distributedsystems.paxos.state.NodeState;
import com.distributedsystems.paxos.util.BallotGenerator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
public class LeaderElectionImpl {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionImpl.class);

    private final NodeState nodeState;
    private final CommitPhaseServiceImpl commitPhaseService;
    private final AcceptPhaseServiceImpl acceptPhaseService;
    private final TransactionBlockRepository transactionBlockRepository;

    private final List<String> members;
    private final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();

    private static final int BASE_ELECTION_TIMEOUT = 3000;
    private static final int ELECTION_JITTER = 2000;

    private static final class QuorumResult {
        final boolean quorumReached; final int acks; final int attempts;
        QuorumResult(boolean quorumReached, int acks, int attempts) {
            this.quorumReached = quorumReached;
            this.acks = acks;
            this.attempts = attempts;
        }
    }

    @Value("${paxos.accept.rpcTimeoutMillis:1000}")
    private long acceptRpcTimeoutMillis;

    @Value("${paxos.accept.maxAttempts:4}")
    private int acceptMaxAttempts;

    @Value("${paxos.accept.baseBackoffMillis:150}")
    private long acceptBaseBackoffMillis;

    @Value("${paxos.accept.jitterMillis:150}")
    private long acceptJitterMillis;

    @Value("${paxos.commit.fanoutRetries:0}")
    private int commitFanoutRetries;

    @Value("${paxos.prepareThrottleMillis:600}")
    private long prepareThrottleMillis;

    public LeaderElectionImpl(NodeState ns,
                              CommitPhaseServiceImpl commitPhaseService,
                              AcceptPhaseServiceImpl acceptSvc,
                              TransactionBlockRepository transactionBlockRepository,
                              @Value("${paxos.members}") String membersConfig) {
        this.nodeState = ns;
        this.commitPhaseService = commitPhaseService;
        this.acceptPhaseService = acceptSvc;
        this.transactionBlockRepository = transactionBlockRepository;

        LinkedHashSet<String> set = new LinkedHashSet<>();
        if (membersConfig != null && !membersConfig.isBlank()) {
            Arrays.stream(membersConfig.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(set::add);
        }
        String selfId = ns.getSelfNodeId();
        if (selfId != null && !selfId.isBlank()) set.add(selfId);
        else log.warn("Self node id not initialized in constructor; proceeding with config-only membership.");

        this.members = new ArrayList<>(set);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("[{}] Node up. Members={}", nodeState.getSelfNodeId(), members);
        nodeState.initClusterMembers(members);
    }

    private List<String> livePeers() {
        List<String> peers = nodeState.getLiveNodes();
        peers.removeIf(p -> Objects.equals(p, nodeState.getSelfNodeId()));
        return peers;
    }

    private int liveQuorum() {
        int n = nodeState.getFullMembership().size();
        return (n / 2) + 1;
    }

    @Scheduled(fixedDelay = 1000)
    public void runLeaderPipeline() {
        if (selfInactiveForSet()) return;

        if (nodeState.isLeader()) {
            sendHeartbeats();

            if (nodeState.hasDeferred()) {
                int liveN = nodeState.getLiveNodes().size();
                int quorum = liveQuorum();
                if (liveN >= quorum) {
                    List<AcceptEntry> pending = nodeState.drainDeferred();
                    log.info("[{}] Replaying {} deferred txns (live={}, quorum={})",
                            nodeState.getSelfNodeId(), pending.size(), liveN, quorum);
                    for (AcceptEntry e : pending) {
                        try {
                            proposeAndCommit(
                                    e.getRequest().getClientId(),
                                    e.getRequest().getTimestamp(),
                                    e.getRequest().getValue().getTransfer().getFrom(),
                                    e.getRequest().getValue().getTransfer().getTo(),
                                    e.getRequest().getValue().getTransfer().getAmount()
                            );
                        } catch (Exception ex) {
                            log.error("[{}] Replay failed for seq={}: {}", nodeState.getSelfNodeId(),
                                    e.getSequence().getValue(), ex.toString());
                            nodeState.enqueueDeferred(e);
                        }
                    }
                } else {
                    log.debug("[{}] Still waiting quorum (live={}, quorum={}) for deferred replay",
                            nodeState.getSelfNodeId(), liveN, quorum);
                }
            }

        } else if (nodeState.electionTimeoutExpired(BASE_ELECTION_TIMEOUT, ELECTION_JITTER)) {
            startElection();
        }
    }

    private void startElection() {
        if (selfInactiveForSet()) return;

        long sincePrepare = System.currentTimeMillis() - nodeState.getLastPrepareSeenAtMillis();
        if (sincePrepare <= prepareThrottleMillis) return;

        long nextBallot = BallotGenerator.nextBallot(selfNodeNumericId());
        long promised = nodeState.getHighestBallotSeen();
        if (nextBallot <= promised) nextBallot = promised + 1;
        nodeState.updateHighestBallot(nextBallot);

        Ballot ballot = Ballot.newBuilder()
                .setNumber(nextBallot)
                .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .build();

        int promises = 1;
        for (String peer : livePeers()) {
            try {
                PromiseReply reply = stub(peer)
                        .withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS)
                        .prepare(PrepareRequest.newBuilder()
                                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                .setBallot(ballot)
                                .build());
                if (!reply.getTimerExpired()) continue;
                if (reply.getBallot().getNumber() == nextBallot) promises++;
            } catch (StatusRuntimeException ignored) {}
        }

        if (promises < liveQuorum()) {
            nodeState.clearLeader();
            return;
        }


        nodeState.setLeaderId(nodeState.getSelfNodeId());
        nodeState.resetLeaderTraffic();

        List<AcceptEntry> allEntries = new ArrayList<>(nodeState.getAcceptedLog());
        for (String peer : livePeers()) {
            try {
                AcceptLogReply reply = stub(peer)
                        .withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS)
                        .getAcceptLog(AcceptLogRequest.newBuilder()
                                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                .build());
                if (reply != null && !reply.getEntriesList().isEmpty()) {
                    allEntries.addAll(reply.getEntriesList());
                }
            } catch (StatusRuntimeException ignored) {}
        }

        List<AcceptEntry> merged = nodeState.mergeAcceptedLogs(allEntries);
        if (merged.isEmpty()) {
            merged = buildEntriesFromDbAsAccepts(nodeState.getSelfNodeId(), nextBallot);
        }
        List<AcceptEntry> filled = nodeState.insertNoOpForGaps(merged, nextBallot, nodeState.getSelfNodeId());
        nodeState.recordNewView(nodeState.getSelfNodeId(), nextBallot, filled);

        if (!filled.isEmpty()) {
            long tip = filled.get(filled.size() - 1).getSequence().getValue();
            nodeState.setLeaderLastCommittedHint(tip);
        }

        fanoutNewView(nextBallot, filled);

        log.info("[{}] Became leader at ballot {} (entries={})",
                nodeState.getSelfNodeId(), nextBallot, filled.size());
    }

    private List<AcceptEntry> buildEntriesFromDbAsAccepts(String leaderId, long ballotNum) {
        try {
            var rows = new ArrayList<>(transactionBlockRepository.findAll());
            rows.sort(Comparator.comparingLong(r -> r.getSequence()));

            List<AcceptEntry> out = new ArrayList<>(rows.size());
            for (var r : rows) {
                out.add(AcceptEntry.newBuilder()
                        .setSequence(Sequence.newBuilder().setValue(r.getSequence()))
                        .setBallot(Ballot.newBuilder()
                                .setNumber(ballotNum)
                                .setProposer(NodeId.newBuilder().setId(leaderId)))
                        .setRequest(ClientRequest.newBuilder()
                                .setClientId(Optional.ofNullable(r.getClientId()).orElse("db-replay"))
                                .setTimestamp(Optional.ofNullable(r.getClientTsMillis()).orElse(0L))
                                .setValue(ClientRequestValue.newBuilder()
                                        .setTransfer(Transfer.newBuilder()
                                                .setFrom(Optional.ofNullable(r.getFromAccountId()).orElse(""))
                                                .setTo(Optional.ofNullable(r.getToAccountId()).orElse(""))
                                                .setAmount(Optional.ofNullable(r.getAmountCents()).orElse(0L)))))
                        .build());
            }
            return out;
        } catch (Exception e) {
            log.warn("[{}] DB fallback for NEW-VIEW failed: {}", nodeState.getSelfNodeId(), e.toString());
            return List.of();
        }
    }

    private void fanoutNewView(long ballotNum, List<AcceptEntry> entries) {
        NewViewRequest.Builder builder = NewViewRequest.newBuilder()
                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setBallot(Ballot.newBuilder()
                        .setNumber(ballotNum)
                        .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                        .build());

        if (entries != null && !entries.isEmpty()) {
            builder.addAllEntries(entries);
        }
        NewViewRequest req = builder.build();

        for (String peer : nodeState.livePeers()) {
            try {
                stub(peer)
                        .withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS)
                        .newView(req);
            } catch (StatusRuntimeException ignored) { }
        }
    }

    public boolean proposeAndCommit(String clientId, long clientTsMillis,
                                    String from, String to, long amount) {
        if (nodeState.isForceFailedForSet() || !nodeState.isLeader() || selfInactiveForSet()) return false;

        long seq = Math.max(nodeState.nextProposalSeq(), nodeState.getLastCommittedSeq() + 1);
        long ballotNum = nodeState.getHighestBallotSeen();

        AcceptEntry entry = AcceptEntry.newBuilder()
                .setSequence(Sequence.newBuilder().setValue(seq))
                .setBallot(Ballot.newBuilder()
                        .setNumber(ballotNum)
                        .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId())))
                .setRequest(ClientRequest.newBuilder()
                        .setClientId(clientId)
                        .setTimestamp(clientTsMillis)
                        .setValue(ClientRequestValue.newBuilder()
                                .setTransfer(Transfer.newBuilder()
                                        .setFrom(from).setTo(to).setAmount(amount))))
                .build();

        nodeState.appendAccepted(seq, from, to, amount, ballotNum, clientId, clientTsMillis);

        QuorumResult qr = gatherAcceptQuorum(entry);
        if (!qr.quorumReached) {
            nodeState.enqueueDeferred(entry);
            log.warn("[{}] Quorum not reached for seq={} → deferred (acks={}/{})",
                    nodeState.getSelfNodeId(), seq, qr.acks, nodeState.getLiveNodes().size());
            return false;
        }

        CommitRequest commitReq = CommitRequest.newBuilder()
                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                .setEntry(entry)
                .build();

        try {
            commitPhaseService.onCommit(commitReq);
            fanoutCommit(commitReq);
            log.info("[{}] COMMIT applied locally for seq={} (acks met)", nodeState.getSelfNodeId(), seq);
            return true;
        } catch (Exception e) {
            log.error("[{}] local commit failed for seq={}: {}", nodeState.getSelfNodeId(), seq, e.toString());
            return false;
        }
    }

    public boolean proposeTransaction(String sender, String receiver, long amount) {
        String clientId = "client-" + ((int) (System.currentTimeMillis() % 10) + 1);
        long clientTs = System.currentTimeMillis();

        boolean success = proposeAndCommit(clientId, clientTs, sender, receiver, amount);
        if (!success)
            log.warn("[{}] proposeTransaction failed for {} -> {} : {}", nodeState.getSelfNodeId(), sender, receiver, amount);
        return success;
    }

    private QuorumResult gatherAcceptQuorum(AcceptEntry entry) {
        final int majority = liveQuorum();
        int attempts = 0;
        while (++attempts <= acceptMaxAttempts) {
            int acks = 1;
            for (String peer : livePeers()) {
                try {
                    AcceptedReply r = stub(peer)
                            .withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS)
                            .accept(AcceptRequest.newBuilder()
                                    .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                    .setEntry(entry).build());
                    if (r.getOk()) acks++;
                } catch (StatusRuntimeException ignored) {}
            }
            if (acks >= majority) return new QuorumResult(true, acks, attempts);
            sleepQuietly(acceptBaseBackoffMillis + ThreadLocalRandom.current().nextLong(acceptJitterMillis + 1));
        }
        return new QuorumResult(false, 0, acceptMaxAttempts);
    }

    private void fanoutCommit(CommitRequest commitReq) {
        for (String peer : livePeers()) {
            try {
                stub(peer).withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS).commit(commitReq);
            } catch (StatusRuntimeException ignored) {}
        }
    }

    private void sendHeartbeats() {
        for (String peer : livePeers()) {
            try {
                stub(peer).withDeadlineAfter(acceptRpcTimeoutMillis, TimeUnit.MILLISECONDS)
                        .heartbeat(HeartbeatRequest.newBuilder()
                                .setFrom(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                .setBallot(Ballot.newBuilder()
                                        .setNumber(nodeState.getHighestBallotSeen())
                                        .setProposer(NodeId.newBuilder().setId(nodeState.getSelfNodeId()))
                                        .build())
                                .setLastCommitted(nodeState.getLastCommittedSeq())
                                .build());
            } catch (StatusRuntimeException ignored) {}
        }
    }

    private PaxosServiceGrpc.PaxosServiceBlockingStub stub(String target) {
        ManagedChannel ch = channelCache.computeIfAbsent(target,
                t -> ManagedChannelBuilder.forTarget(t).usePlaintext().build());
        return PaxosServiceGrpc.newBlockingStub(ch);
    }

    @PreDestroy
    public void shutdownAll() {
        channelCache.values().forEach(ch -> {
            ch.shutdown();
            try {
                if (!ch.awaitTermination(3, TimeUnit.SECONDS)) ch.shutdownNow();
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
    }

    private void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    private boolean selfInactiveForSet() {
        return !nodeState.isSelfLive() || nodeState.isForceFailedForSet() || nodeState.areTimersPaused();
    }

    private int selfNodeNumericId() {
        try {
            String id = nodeState.getSelfNodeId();
            String[] parts = id.split(":");
            int port = Integer.parseInt(parts[1]);
            int idx = port - 9090;
            if (idx < 0 || idx > 255) idx = Math.floorMod(port, 256);
            return idx;
        } catch (Exception e) {
            return 1;
        }
    }
}
