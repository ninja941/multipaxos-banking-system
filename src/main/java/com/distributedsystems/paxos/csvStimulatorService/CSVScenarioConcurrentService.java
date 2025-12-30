package com.distributedsystems.paxos.csvStimulatorService;

import com.distributedsystems.paxos.apiGateway.ClientRouter;
import com.distributedsystems.paxos.apiGateway.dto.ClientTransactionResponse;
import com.distributedsystems.paxos.state.NodeState;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class CSVScenarioConcurrentService {

    private static final Logger log = LoggerFactory.getLogger(CSVScenarioConcurrentService.class);

    private final NodeState ns;
    private final ClientRouter client;
    private final Queue<String> retryBuffer = new ConcurrentLinkedQueue<>();

    public CSVScenarioConcurrentService(NodeState ns, ClientRouter client) {
        this.ns = ns;
        this.client = client;
    }

    public String runCsvScenarioConcurrent(String csvPath, int numClients) {
        final String self = ns.getSelfNodeId();
        log.info("[{}] Running CSV scenario concurrently from {} with {} clients", self, csvPath, numClients);

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, numClients));

        try (Reader in = resolveCsvReader(csvPath)) {
            if (in == null) return "Error: CSV file not found → " + csvPath;

            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim(true)
                    .withAllowMissingColumnNames()
                    .parse(in);

            List<String> clientIds = new ArrayList<>();
            for (int i = 1; i <= numClients; i++) clientIds.add("client-" + i);

            AtomicLong tsGen = new AtomicLong(System.currentTimeMillis());

            int setIdx = 0;
            for (CSVRecord record : records) {
                setIdx++;
                String setNumber = safe(record, "Set Number");
                String txListRaw = safe(record, "Transactions");
                String liveNodesRaw = safe(record, "Live Nodes");

                if (txListRaw.isBlank() && liveNodesRaw.isBlank()) {
                    log.warn("[{}] Row {} is empty → skipping.", self, setIdx);
                    continue;
                }

                final int fSetIdx = setIdx;
                final String fSetNumber = setNumber;
                final String fLiveNodesRaw = liveNodesRaw;
                final String fTxListRaw = txListRaw;

                pauseLogsDuringPrompt(() -> {
                    System.err.println("Ready to execute SET " + fSetIdx + " (Set Number: " +
                            (fSetNumber.isBlank() ? fSetIdx : fSetNumber) + ")");
                    System.err.println("Live Nodes: " + fLiveNodesRaw);
                    System.err.println("Transactions: " + fTxListRaw);
                    System.err.println("Press ENTER to start this set.");
                    try {
                        Scanner sc = new Scanner(System.in);
                        String cmd = sc.nextLine().trim();
                        if ("lf".equalsIgnoreCase(cmd)) simulateLeaderFailureBeforeSet(fSetIdx);
                    } catch (Exception e) {
                        log.warn("[{}] User prompt skipped (non-interactive env).", self);
                    }
                });

                // ---- process live nodes ----
                Set<String> liveSet = parseLiveNodes(liveNodesRaw);
                if (liveSet.isEmpty()) {
                    log.warn("[{}] Set {} has no live nodes → skipping.", self, setNumber);
                    continue;
                }

                log.info("\n=== [SET {}] Live Nodes: {} ===", (setNumber.isBlank() ? setIdx : setNumber), liveSet);

                ns.recoverAfterSet();
                client.broadcastCsvLiveOverride(liveSet);
                client.broadcastPauseExcept(liveSet);
                client.broadcastClearLeader(liveSet);
                client.broadcastResume(liveSet);

                List<String> tokens = tokenize(txListRaw);
                if (!retryBuffer.isEmpty()) {
                    log.warn("[{}] Replaying {} uncommitted TXs from previous set.", self, retryBuffer.size());
                    tokens.addAll(0, new ArrayList<>(retryBuffer));
                    retryBuffer.clear();
                }

                int quorum = (ns.getFullMembership().size() / 2) + 1;
                if (liveSet.size() < quorum) {
                    log.warn("[{}] Skipping SET {} — insufficient live nodes for quorum (live={} < quorum={}). Deferring {} tx(s) to next set.",
                            self, setIdx, liveSet.size(), quorum, tokens.size());

                    if (!tokens.isEmpty()) retryBuffer.addAll(tokens);

                    ns.pauseAllTimers();
                    client.broadcastCsvClearOverride();
                    ns.clearCsvLiveOverride();
                    log.info("[{}] === Deferred SET {} ===", self, setIdx);
                    continue;
                }

                sleepQuietly(800);
                client.waitForLeaderInSet(liveSet, 20000);
                log.info("[{}] Leader ready for SET {}", self, setIdx);
                sleepQuietly(300);

                if (!tokens.isEmpty()) {
                    List<String> failed = runTokens(tokens, pool, clientIds, tsGen);
                    if (!failed.isEmpty()) {
                        log.warn("[{}] SET {} had {} uncommitted TXs → will retry next set.", self, setIdx, failed.size());
                        retryBuffer.addAll(failed);
                    }
                } else {
                    log.warn("[{}] No transactions in SET {} → continuing.", self, setIdx);
                }

                ns.pauseAllTimers();
                client.broadcastCsvClearOverride();
                ns.clearCsvLiveOverride();
                sleepQuietly(250);
                log.info("[{}] === Completed SET {} ===", self, setIdx);

                pauseLogsDuringPrompt(() -> {
                    System.err.println("SET " + fSetIdx + " completed.");
                    System.err.println("Press ENTER to proceed to next set.");
                    new Scanner(System.in).nextLine();
                });

                try {
                    Set<String> all = new LinkedHashSet<>(ns.getFullMembership());
                    client.broadcastCsvLiveOverride(all);
                    client.broadcastClearLeader(all);
                    client.broadcastResume(all);
                    client.waitForLeaderInSet(all, 20000);
                    sleepQuietly(1000);
                    log.info("[{}] Full cluster stabilized post SET {}", self, setIdx);
                } catch (Exception e) {
                    log.error("[{}] Post-SET recovery failed: {}", self, e.toString());
                }
            }

            ns.pauseAllTimers();
            client.broadcastCsvClearOverride();
            ns.clearCsvLiveOverride();

            return "CSV concurrent scenario completed successfully.";
        } catch (Exception e) {
            log.error("[{}] Concurrent CSV error: {}", self, e.toString(), e);
            return "Error running scenario: " + e.getMessage();
        } finally {
            pool.shutdown();
        }
    }

    private void simulateLeaderFailureBeforeSet(int setIdx) {
        final String self = ns.getSelfNodeId();
        log.info("[{}] Pre-set leader failure requested for SET {}", self, setIdx);
        Set<String> liveNodes = ns.getCsvLiveOverride().isEmpty()
                ? new LinkedHashSet<>(ns.getLiveNodes())
                : new LinkedHashSet<>(ns.getCsvLiveOverride());
        if (liveNodes.isEmpty()) liveNodes = new LinkedHashSet<>(ns.getFullMembership());
        if (!liveNodes.isEmpty()) {
            String failed = liveNodes.iterator().next();
            liveNodes.remove(failed);
            log.info("[{}] Simulated pre-set leader {} failure → remaining {}", self, failed, liveNodes);
            client.broadcastCsvLiveOverride(liveNodes);
            client.broadcastClearLeader(liveNodes);
            client.broadcastResume(liveNodes);
            client.triggerElection();
            client.waitForLeaderInSet(liveNodes, 12000);
        }
    }

    private List<String> runTokens(List<String> tokens,
                                   ExecutorService pool,
                                   List<String> clientIds,
                                   AtomicLong tsGen) throws InterruptedException {
        int rr = 0, idx = 0;
        List<String> failedTxs = new ArrayList<>();

        while (idx < tokens.size()) {
            String token = tokens.get(idx);


            if (isLf(token)) {
                try {
                    log.info("[{}] LF encountered — simulating leader failure.", ns.getSelfNodeId());
                    Set<String> liveNodes = ns.getCsvLiveOverride().isEmpty()
                            ? new LinkedHashSet<>(ns.getLiveNodes())
                            : new LinkedHashSet<>(ns.getCsvLiveOverride());

                    if (!liveNodes.isEmpty()) {
                        String failed = liveNodes.iterator().next();
                        liveNodes.remove(failed);
                        log.info("[{}] Simulated leader {} failure → remaining {}", ns.getSelfNodeId(), failed, liveNodes);
                        if (liveNodes.isEmpty()) {
                            liveNodes.addAll(ns.getFullMembership());
                            log.warn("[{}] All nodes were down → restored full cluster.", ns.getSelfNodeId());
                        }
                    }

                    client.broadcastCsvLiveOverride(liveNodes);
                    client.broadcastClearLeader(liveNodes);
                    client.broadcastResume(liveNodes);
                    client.triggerElection();
                    client.waitForLeaderInSet(liveNodes, 12000);

                    Set<String> full = new LinkedHashSet<>(ns.getFullMembership());
                    client.broadcastCsvLiveOverride(full);
                    client.broadcastClearLeader(full);
                    client.broadcastResume(full);
                    client.waitForLeaderInSet(full, 20000);
                    sleepQuietly(1200);

                    log.info("[{}] LF recovery done — continuing transactions.", ns.getSelfNodeId());
                } catch (Exception e) {
                    log.error("[{}] LF handling failed: {}", ns.getSelfNodeId(), e.toString());
                }

                idx++;
                continue;
            }

            final String tx = token.trim();
            if (tx.isEmpty()) { idx++; continue; }

            final String[] parts = tx.split(",");
            if (parts.length != 3) { idx++; continue; }

            final String from = parts[0].trim();
            final String to = parts[1].trim();
            final long amount = Long.parseLong(parts[2].trim());
            final String clientId = clientIds.get(rr % clientIds.size());
            rr++;
            final long clientTs = tsGen.incrementAndGet();

            log.info("[TX] {} → {} : {}", from, to, amount);
            Future<?> task = pool.submit(() -> {
                try {
                    ClientTransactionResponse r = client.submitWithRetry(from, to, amount, clientId, clientTs, 3);
                    if (!r.issuccess()) failedTxs.add(tx);
                } catch (Exception e) {
                    failedTxs.add(tx);
                }
            });

            try {
                task.get(45, TimeUnit.SECONDS);
            } catch (TimeoutException | ExecutionException e) {
                failedTxs.add(tx);
            }

            idx++;
        }

        return failedTxs;
    }

    private void pauseLogsDuringPrompt(Runnable promptAction) {
        try {
            System.err.flush();
            promptAction.run();
        } catch (Exception e) {
            log.error("Prompt display error: {}", e.toString());
        }
    }


    private static List<String> tokenize(String txList) {
        List<String> out = new ArrayList<>();
        if (txList == null || txList.isBlank()) return out;
        String[] chunks = txList.replace("\"", "").trim().split(";");
        for (String raw : chunks) {
            String t = raw.replace("(", "").replace(")", "").trim();
            if (!t.isEmpty()) out.add(t.equalsIgnoreCase("lf") ? "lf" : t);
        }
        return out;
    }

    private static boolean isLf(String token) { return "lf".equalsIgnoreCase(token); }

    private Reader resolveCsvReader(String path) {
        try {
            if (path.startsWith("classpath:")) {
                InputStream s = getClass().getClassLoader()
                        .getResourceAsStream(path.replace("classpath:", "").trim());
                return s == null ? null : new InputStreamReader(s, StandardCharsets.UTF_8);
            } else {
                return new FileReader(path, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            return null;
        }
    }

    private Set<String> parseLiveNodes(String liveNodesStr) {
        Set<String> out = new LinkedHashSet<>();
        if (liveNodesStr == null || liveNodesStr.isBlank()) return out;
        String body = liveNodesStr.replace("[", "").replace("]", "").replace("\"", "").trim();
        if (body.isEmpty()) return out;
        for (String n : body.split(",")) {
            String t = n.trim().toLowerCase();
            if (t.isEmpty()) continue;
            out.add("localhost:" + switch (t) {
                case "n1" -> 9091;
                case "n2" -> 9092;
                case "n3" -> 9093;
                case "n4" -> 9094;
                case "n5" -> 9095;
                default -> throw new IllegalArgumentException("Unknown node: " + t);
            });
        }
        return out;
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static String safe(CSVRecord r, String key) {
        try { return Optional.ofNullable(r.get(key)).orElse("").trim(); }
        catch (IllegalArgumentException e) { return ""; }
    }
}
