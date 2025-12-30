package com.distributedsystems.paxos.state;

import com.distributedsystems.paxos.Repository.CustomerAccountRepository;
import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.model.CustomerAccount;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.proto.Paxos;
import com.distributedsystems.paxos.proto.Paxos.AcceptEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
public class NodeState {
    private static final Logger log = LoggerFactory.getLogger(NodeState.class);

    private final CustomerAccountRepository customerAccountRepository;
    private final TransactionBlockRepository txRepo;
    private final TransactionTemplate txTemplate;

    private volatile List<String> fullMembership = new ArrayList<>();
    private volatile List<String> liveNodes = new ArrayList<>();
    private volatile boolean csvLiveOverrideActive = false;

    private volatile String selfNodeId;
    private volatile String leaderId;
    private volatile long highestBallotSeen = -1L;
    private volatile long lastLeaderTrafficTs = System.currentTimeMillis();

    private final AtomicLong nextSeq = new AtomicLong(1);
    private volatile long lastCommittedSeq = 0L;
    private volatile long lastExecutedSeq  = 0L;

    private final List<AcceptEntry> acceptedLog = Collections.synchronizedList(new ArrayList<>());
    private final NavigableMap<Long, TransactionBlock> commitBuffer = new ConcurrentSkipListMap<>();
    private volatile long lastIssuedBallot = -1L;

    private final AtomicBoolean csvForceFail = new AtomicBoolean(false);
    private final AtomicBoolean timersPaused = new AtomicBoolean(false);

    private final AtomicLong lastElectionActivityAtMillis = new AtomicLong(System.currentTimeMillis());

    private volatile long lastPrepareSeenAtMillis = System.currentTimeMillis();

    private volatile long leaderLastCommittedHint = 0L;
    public long getLeaderLastCommittedHint() { return leaderLastCommittedHint; }
    public void setLeaderLastCommittedHint(long v) { leaderLastCommittedHint = Math.max(leaderLastCommittedHint, v); }

    @org.springframework.beans.factory.annotation.Value("${paxos.startupGrace.ms:5000}")
    private long startupGraceMs;
    private volatile long startupGraceUntilMillis;

    @Autowired
    public NodeState(CustomerAccountRepository customerAccountRepository,
                     TransactionBlockRepository txRepo,
                     PlatformTransactionManager txManager) {
        this.customerAccountRepository = customerAccountRepository;
        this.txRepo = txRepo;
        this.txTemplate = new TransactionTemplate(txManager);
        this.txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }


    private static String normalize(String id) {
        if (id == null) return null;
        return id.replace("[0:0:0:0:0:0:0:1]", "localhost")
                .replace("127.0.0.1", "localhost")
                .trim();
    }

    public void initSelf(String grpcAddr) {
        this.selfNodeId = normalize(grpcAddr);
        this.lastLeaderTrafficTs = System.currentTimeMillis();
        log.info("NodeState initialized with selfNodeId={}", selfNodeId);
    }

    public synchronized void initClusterMembers(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) return;

        List<String> norm = nodes.stream().map(NodeState::normalize).toList();

        // Always include self in membership
        if (selfNodeId != null && !norm.contains(selfNodeId)) {
            norm = new java.util.ArrayList<>(norm);
            ((java.util.ArrayList<String>) norm).add(selfNodeId);
        }

        if (this.fullMembership.isEmpty()) {
            this.fullMembership = new ArrayList<>(norm);
            log.info("[{}] Full membership initialized: {}", selfNodeId, this.fullMembership);
        }

        if (!csvLiveOverrideActive) {
            this.liveNodes = new ArrayList<>(norm);
            log.info("[{}] Live nodes set to {}", selfNodeId, this.liveNodes);
        }

        this.startupGraceUntilMillis = System.currentTimeMillis() + Math.max(0, startupGraceMs);
    }

    public synchronized void setLiveNodes(List<String> nodes) {
        List<String> norm = (nodes == null) ? List.of() : nodes.stream().map(NodeState::normalize).toList();
        if (this.fullMembership.isEmpty() && !norm.isEmpty()) this.fullMembership = new ArrayList<>(norm);
        this.liveNodes = new ArrayList<>(norm);
        this.csvLiveOverrideActive = false;
        log.info("[{}] Live nodes set to {}", selfNodeId, this.liveNodes);
    }

    public synchronized void overrideLiveNodesForCsv(Collection<String> nodes) {
        List<String> norm = (nodes == null) ? List.of() : nodes.stream().map(NodeState::normalize).toList();
        this.liveNodes = new ArrayList<>(norm);
        this.csvLiveOverrideActive = true;
        if (this.fullMembership.isEmpty()) this.fullMembership = new ArrayList<>(liveNodes);

        boolean iAmLive = isNodeLive(selfNodeId);
        if (!iAmLive) {
            stepDown("excluded by CSV set");
            csvForceFail.set(true);
            pauseAllTimers();
            leaderId = null;
        } else {
            csvForceFail.set(false);
        }

        log.info("[{}] CSV override: Live nodes -> {} (selfLive={})", selfNodeId, this.liveNodes, iAmLive);
    }

    public synchronized void clearCsvLiveOverride() {
        if (!fullMembership.isEmpty()) {
            this.liveNodes = new ArrayList<>(fullMembership);
        }
        this.csvLiveOverrideActive = false;
        csvForceFail.set(false);
        log.info("[{}] CSV override cleared. Live nodes -> {}", selfNodeId, this.liveNodes);
    }

    public String getSelfNodeId() { return selfNodeId; }

    public List<String> getLiveNodes() { return new ArrayList<>(liveNodes); }

    public List<String> getFullMembership() { return new ArrayList<>(fullMembership); }

    public boolean isNodeLive(String nodeId) {
        List<String> ls = this.liveNodes;
        return nodeId != null && ls != null && !ls.isEmpty() && ls.contains(nodeId);
    }

    public boolean isSelfLive() { return isNodeLive(selfNodeId); }

    public List<String> livePeers() {
        List<String> ls = getLiveNodes();
        ls.removeIf(n -> Objects.equals(n, selfNodeId));
        return ls;
    }

    public boolean shouldSendTo(String peer) { return isNodeLive(peer); }

    public void setLeaderId(String id) { this.leaderId = id; }
    public String getLeaderId() { return leaderId; }
    public void clearLeader() { this.leaderId = null; }
    public boolean isLeader() { return selfNodeId != null && selfNodeId.equals(leaderId); }

    public int clusterSize() {
        List<String> ls = this.liveNodes;
        return (ls == null || ls.isEmpty()) ? 1 : ls.size();
    }

    public int quorum() {
        int n = clusterSize();
        return (n / 2) + 1;
    }

    public synchronized long nextBallot() {
        long nodeBits = Math.abs(Objects.hashCode(selfNodeId)) & 0xFFFF;      // 16b node suffix
        long timeBits = (System.currentTimeMillis() & 0x7FFFFFFFL) << 16;     // 31b time prefix
        long candidate = timeBits | nodeBits;
        if (candidate <= lastIssuedBallot) candidate = lastIssuedBallot + 1;
        lastIssuedBallot = candidate;
        return candidate;
    }

    public long getHighestBallotSeen() { return highestBallotSeen; }
    public void updateHighestBallot(long b) { this.highestBallotSeen = Math.max(highestBallotSeen, b); }

    public void setLastLeaderTrafficTs(long ts) { this.lastLeaderTrafficTs = ts; }
    public void resetLeaderTraffic() { this.lastLeaderTrafficTs = System.currentTimeMillis(); }

    public boolean isFollowerTimerExpired(long timeoutMs) {
        long now = System.currentTimeMillis();
        if (now < startupGraceUntilMillis) return true;   // allow instant PREPARE at boot
        long last = this.lastLeaderTrafficTs;
        return (now - last) > Math.max(250L, timeoutMs);
    }

    public boolean electionTimeoutExpired(long baseMs, long jitterMs) {
        if (areTimersPaused() || isForceFailedForSet()) return false;
        long elapsed = System.currentTimeMillis() - lastLeaderTrafficTs;
        long timeout = baseMs + (jitterMs > 0 ? new Random().nextInt((int) jitterMs) : 0);
        return elapsed > timeout;
    }

    public void touchLastElectionActivityNow() {
        long now = System.currentTimeMillis();
        lastElectionActivityAtMillis.set(now);
        lastPrepareSeenAtMillis = now;
    }

    public long getLastElectionActivityAtMillis() { return lastElectionActivityAtMillis.get(); }

    public void failForCurrentSet() {
        csvForceFail.set(true);
        pauseAllTimers();
    }

    public void recoverAfterSet() {
        csvForceFail.set(false);
        resumeTimers();
    }

    public boolean isForceFailedForSet() { return csvForceFail.get(); }

    public void pauseAllTimers() { timersPaused.set(true); }

    public void resumeTimers() {
        timersPaused.set(false);
        resetElectionTimer();
    }

    public boolean areTimersPaused() { return timersPaused.get(); }

    private void resetElectionTimer() {
        touchLastElectionActivityNow();
    }

    public synchronized void stepDown(String reason) {
        if (isLeader()) {
            log.warn("[{}] stepping down: {}", selfNodeId, reason);
        }
        clearLeader();
    }



    public void acceptValue(AcceptEntry entry) {
        acceptedLog.add(entry);
        updateHighestBallot(entry.getBallot().getNumber());
        log.debug("[{}] Accepted entry seq={} ballot={}", selfNodeId,
                entry.getSequence().getValue(), entry.getBallot().getNumber());
    }

    public List<AcceptEntry> getAcceptedLog() {
        synchronized (acceptedLog) {
            return new ArrayList<>(acceptedLog);
        }
    }

    public synchronized List<AcceptEntry> mergeAcceptedLogs(List<AcceptEntry> all) {
        if (all == null || all.isEmpty()) return List.of();
        Map<Long, AcceptEntry> merged = new HashMap<>();
        for (AcceptEntry e : all) {
            long seq = e.getSequence().getValue();
            AcceptEntry cur = merged.get(seq);
            if (cur == null || e.getBallot().getNumber() > cur.getBallot().getNumber()) {
                merged.put(seq, e);
            }
        }
        return merged.values().stream()
                .sorted(Comparator.comparingLong(a -> a.getSequence().getValue()))
                .toList();
    }

    public synchronized List<AcceptEntry> insertNoOpForGaps(List<AcceptEntry> merged, long ballot, String leaderId) {
        if (merged == null || merged.isEmpty()) return List.of();
        long maxSeq = merged.getLast().getSequence().getValue();

        Map<Long, AcceptEntry> bySeq = merged.stream()
                .collect(Collectors.toMap(e -> e.getSequence().getValue(), e -> e));

        List<AcceptEntry> filled = new ArrayList<>((int) maxSeq);
        for (long s = 1; s <= maxSeq; s++) {
            AcceptEntry e = bySeq.get(s);
            if (e != null) {
                filled.add(e);
            } else {
                AcceptEntry noop = AcceptEntry.newBuilder()
                        .setSequence(Paxos.Sequence.newBuilder().setValue(s))
                        .setBallot(Paxos.Ballot.newBuilder()
                                .setNumber(ballot)
                                .setProposer(Paxos.NodeId.newBuilder().setId(leaderId)))
                        .setRequest(Paxos.ClientRequest.newBuilder()
                                .setClientId("noop-client")
                                .setTimestamp(System.currentTimeMillis())
                                .setValue(Paxos.ClientRequestValue.newBuilder())) // empty op
                        .build();
                filled.add(noop);
            }
        }
        return filled;
    }

    public synchronized void replaceCommittedLog(List<AcceptEntry> entries) {
        acceptedLog.clear();
        if (entries != null && !entries.isEmpty()) {
            acceptedLog.addAll(entries);
            long maxSeq = entries.stream().mapToLong(e -> e.getSequence().getValue()).max().orElse(0L);
            lastCommittedSeq = Math.max(lastCommittedSeq, maxSeq);
        }
        log.info("[{}] replaceCommittedLog: acceptedEntries={}, lastCommittedSeq={}",
                selfNodeId, acceptedLog.size(), lastCommittedSeq);
    }


    public void appendAccepted(long seq,
                               String from,
                               String to,
                               long amount,
                               long ballot,
                               String clientId,
                               long clientTsMillis) {

        txRepo.findBySequence(seq).orElseGet(() ->
                txRepo.save(TransactionBlock.builder()
                        .sequence(seq)
                        .fromAccountId(from)
                        .toAccountId(to)
                        .amountCents(amount)
                        .ballot(ballot)
                        .clientId(clientId)
                        .clientTsMillis(clientTsMillis)
                        .build()));
    }

    public synchronized void markCommitted(long seq, TransactionBlock maybeBlock) {
        TransactionBlock block = txRepo.findBySequence(seq).orElseGet(() -> {
            TransactionBlock b = (maybeBlock != null) ? maybeBlock : new TransactionBlock();
            b.setSequence(seq);
            return txRepo.save(b);
        });

        if (block.getCommittedAt() == null) {
            block.setCommittedAt(Instant.now());
            txRepo.save(block);
        }

        lastCommittedSeq = Math.max(lastCommittedSeq, seq);
        commitBuffer.put(seq, block);
    }

    public synchronized void executeReady() {
        while (true) {
            long next = lastExecutedSeq + 1;
            TransactionBlock tb = commitBuffer.get(next);

            if (tb == null) {
                log.debug("[{}] executeReady: next seq {} not in buffer", selfNodeId, next);
                break;
            }
            if (tb.getCommittedAt() == null) {
                log.debug("[{}] executeReady: seq {} not committed yet", selfNodeId, next);
                break;
            }
            if (tb.getExecutedAt() != null) {
                commitBuffer.remove(next);
                lastExecutedSeq = next;
                continue;
            }

            boolean ok = applyAndMarkExecutedInTx(tb);
            if (!ok) {
                log.warn("[{}] executeReady: seq {} apply failed; will retry later", selfNodeId, next);
                break;
            }

            TransactionBlock refreshed = txRepo.findBySequence(next).orElse(tb);
            if (refreshed.getExecutedAt() == null) {
                log.warn("[{}] executeReady: seq {} executedAt still null after apply; stopping to retry", selfNodeId, next);
                break;
            }

            lastExecutedSeq = next;
            commitBuffer.remove(next);
            log.info("[{}] Executed seq={}", selfNodeId, next);
        }
    }

    private boolean applyAndMarkExecutedInTx(TransactionBlock tb) {
        try {
            txTemplate.execute(status -> {
                TransactionBlock row = txRepo.findBySequence(tb.getSequence()).orElse(tb);

                if (row.getExecutedAt() != null) return null; // idempotent

                apply(row.getFromAccountId(), row.getToAccountId(), row.getAmountCents());

                row.setExecutedAt(Instant.now());
                txRepo.save(row);
                return null;
            });
            return true;
        } catch (Exception e) {
            log.error("[{}] applyAndMarkExecutedInTx: seq={} failed: {}", selfNodeId, tb.getSequence(), e.toString(), e);
            return false;
        }
    }

    private void apply(String from, String to, long amount) {
        CustomerAccount a = customerAccountRepository.findByName(from)
                .orElseGet(() -> {
                    CustomerAccount acc = new CustomerAccount();
                    acc.setName(from);
                    acc.setBalance(0L);
                    return customerAccountRepository.save(acc);
                });
        CustomerAccount b = customerAccountRepository.findByName(to)
                .orElseGet(() -> {
                    CustomerAccount acc = new CustomerAccount();
                    acc.setName(to);
                    acc.setBalance(0L);
                    return customerAccountRepository.save(acc);
                });

        a.setBalance(a.getBalance() - amount);
        b.setBalance(b.getBalance() + amount);

        customerAccountRepository.save(a);
        customerAccountRepository.save(b);
    }



    public long nextProposalSeq() { return nextSeq.getAndIncrement(); }
    public long getLastCommittedSeq() { return lastCommittedSeq; }
    public long getLastExecutedSeq() { return lastExecutedSeq; }

    public long getLastPrepareSeenAtMillis() { return lastPrepareSeenAtMillis; }
    public void markPrepareSeenNow() { this.lastPrepareSeenAtMillis = System.currentTimeMillis(); }


    private volatile boolean readyForNewWork = true;
    public boolean readyForNewWork() { return readyForNewWork; }
    public void setReadyForNewWork(boolean v) { readyForNewWork = v; }

    private final Deque<Map<String,Object>> newViewHistory = new ArrayDeque<>();


public synchronized void recordNewView(String leader, long ballot, List<Paxos.AcceptEntry> entries) {
    if (newViewHistory.size() > 20) newViewHistory.removeFirst();
    newViewHistory.addLast(Map.of(
            "leader", leader,
            "ballot", ballot,
            "entries", entries.size(),
            "entryList", entries,
            "ts", System.currentTimeMillis()
    ));
}


    private final Deque<AcceptEntry> deferredQueue = new ArrayDeque<>();

    public void enqueueDeferred(AcceptEntry e) {
        log.info("[{}] Queued deferred transaction seq={} (clientId={}, from={}, to={}, amount={})",
                selfNodeId, e.getSequence().getValue(),
                e.getRequest().getClientId(),
                e.getRequest().getValue().getTransfer().getFrom(),
                e.getRequest().getValue().getTransfer().getTo(),
                e.getRequest().getValue().getTransfer().getAmount());
        deferredQueue.add(e);
    }

    public List<AcceptEntry> drainDeferred() {
        List<AcceptEntry> list = new ArrayList<>(deferredQueue);
        deferredQueue.clear();
        return list;
    }

    public boolean hasDeferred() { return !deferredQueue.isEmpty(); }

    public synchronized List<Map<String,Object>> getNewViewHistory() {
        return new ArrayList<>(newViewHistory);
    }
    public Set<String> getCsvLiveOverride() {
        return csvLiveOverrideActive ? new LinkedHashSet<>(liveNodes) : Collections.emptySet();
    }

}
