package com.distributedsystems.paxos.apiGateway;

import com.distributedsystems.paxos.apiGateway.dto.ClientTransactionResponse;
import com.distributedsystems.paxos.apiGateway.dto.ClientTransactionRequest;
import com.distributedsystems.paxos.state.NodeState;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
public class ClientRouter {
    private static final Logger log = LoggerFactory.getLogger(ClientRouter.class);

    private final NodeState ns;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(1500))
            .build();
    private final Random rnd = new Random();
    private final List<String> membersFromConfig;

    public ClientRouter(NodeState ns, @Value("${paxos.nodes:}") String membersConfig) {
        this.ns = ns;
        this.membersFromConfig = parseMembers(membersConfig);
    }

    public ClientTransactionResponse submitWithRetry(
            String from, String to, long amount,
            String clientId, long clientTsMillis, int maxRetries
    ) {
        ClientTransactionRequest req = new ClientTransactionRequest(from, to, amount);
        req.setClientId(clientId);
        req.setClientTsMillis(clientTsMillis);

        String target = pickInitialTarget();
        for (int i = 0; i < maxRetries; i++) {
            ClientTransactionResponse r = httpPostSubmit(target, from, to, amount, clientId, clientTsMillis);
            if (r.issuccess()) return r;

            String hint = normalize(r.getLeaderId());
            if (hint != null && !hint.isBlank() && !hint.equals(normalize(target))) {
                target = hint;
                continue;
            }
            sleep(150 + 100 * i);
        }
        return ClientTransactionResponse.fail(null, "no_leader_reachable");
    }

    public void failLeader() {
        String leaderGrpc = discoverLeaderOrThrow();
        String httpBase = httpBaseForGrpc(leaderGrpc);
        httpPostNoBody(httpBase + "/admin/lf");
    }

    /** Broadcast live nodes to all. */
    public void broadcastCsvLiveOverride(Collection<String> grpcNodes) {
        try {
            byte[] body = mapper.writeValueAsBytes(grpcNodes);
            for (String member : allMembersFallback()) {
                String url = httpBaseForGrpc(member) + "/admin/csv/live";
                httpPost(url, body, "application/json");
            }
        } catch (Exception e) {
            throw new RuntimeException("serialize live-set failed", e);
        }
    }

    public void broadcastCsvClearOverride() {
        for (String member : allMembersFallback()) {
            String url = httpBaseForGrpc(member) + "/admin/csv/clear";
            httpPost(url, new byte[0], "text/plain");
        }
    }

    public void broadcastResume(Collection<String> grpcNodes) {
        for (String member : grpcNodes) {
            String url = httpBaseForGrpc(member) + "/admin/resume";
            httpPostNoBody(url);
        }
    }

    public void broadcastPauseExcept(Collection<String> liveGrpc) {
        Set<String> live = new HashSet<>(liveGrpc);
        for (String member : allMembersFallback()) {
            if (!live.contains(member)) {
                String url = httpBaseForGrpc(member) + "/admin/pause";
                httpPostNoBody(url);
            }
        }
    }

    public void broadcastClearLeader(Collection<String> grpcNodes) {
        for (String g : grpcNodes) {
            String url = httpBaseForGrpc(g) + "/admin/clearLeader";
            httpPostNoBody(url);
        }
    }

    public void waitForLeaderInSet(Collection<String> candidateGrpc, long timeoutMs) {
        long deadline = System.currentTimeMillis() + Math.max(1500, timeoutMs);
        Set<String> allowed = new HashSet<>(candidateGrpc);
        List<String> bases = new ArrayList<>();
        for (String g : allowed) bases.add(httpBaseForGrpc(g));

        while (System.currentTimeMillis() < deadline) {
            for (String base : bases) {
                try {
                    String leader = httpGet(base + "/admin/whois").trim();
                    if (!leader.isBlank() && allowed.contains(leader)) {
                        ns.setLeaderId(leader);
                        return;
                    }
                } catch (Exception ignored) { }
            }
            sleep(200);
        }
        throw new IllegalStateException("No leader elected in set " + allowed + " within " + timeoutMs + "ms");
    }

    public String discoverLeaderOrThrow() {
        if (ns.isLeader()) return ns.getSelfNodeId();
        String known = ns.getLeaderId();
        if (notBlank(known)) return known;

        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        if (notBlank(ns.getSelfNodeId())) candidates.add(ns.getSelfNodeId());
        candidates.addAll(safeList(ns.getLiveNodes()));
        candidates.addAll(safeList(ns.getFullMembership()));
        candidates.addAll(membersFromConfig);

        for (String peerGrpc : candidates) {
            try {
                String httpBase = httpBaseForGrpc(peerGrpc);
                String leader = httpGet(httpBase + "/admin/whois").trim();
                if (!leader.isEmpty()) return leader;
            } catch (Exception ignored) {}
        }
        throw new IllegalStateException("Cannot determine current leader; tried=" + candidates);
    }

    private ClientTransactionResponse httpPostSubmit(
            String targetGrpc, String from, String to,
            long amount, String clientId, long clientTsMillis) {
        try {
            ClientTransactionRequest req = new ClientTransactionRequest(from, to, amount);
            req.setClientId(clientId);
            req.setClientTsMillis(clientTsMillis);
            return trySendToNode(targetGrpc, req);
        } catch (Exception e) {
            log.warn("httpPostSubmit to {} failed: {}", targetGrpc, e.toString());
            ClientTransactionResponse r = new ClientTransactionResponse();
            r.setsuccess(false);
            r.setMessage("send failed: " + e.getMessage());
            return r;
        }
    }

    private ClientTransactionResponse trySendToNode(String nodeGrpc, ClientTransactionRequest req) throws Exception {
        String httpBase = httpBaseForGrpc(nodeGrpc);
        String url = httpBase + "/api/client/submit";
        String json = mapper.writeValueAsString(req);
        HttpRequest httpReq = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(4000))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = http.send(httpReq, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        String body = resp.body() == null ? "" : resp.body().trim();

        if (resp.statusCode() / 100 != 2) {
            ClientTransactionResponse r = new ClientTransactionResponse();
            r.setsuccess(false);
            r.setMessage("HTTP " + resp.statusCode() + " from " + url);
            return r;
        }
        try {
            return mapper.readValue(body, ClientTransactionResponse.class);
        } catch (Exception e) {
            ClientTransactionResponse r = new ClientTransactionResponse();
            r.setsuccess(false);
            r.setMessage("Invalid JSON: " + body);
            return r;
        }
    }

    private String pickInitialTarget() {
        LinkedHashSet<String> ordered = new LinkedHashSet<>();
        if (notBlank(ns.getLeaderId())) ordered.add(ns.getLeaderId());
        if (notBlank(ns.getSelfNodeId())) ordered.add(ns.getSelfNodeId());
        ordered.addAll(safeList(ns.getLiveNodes()));
        ordered.addAll(safeList(ns.getFullMembership()));
        ordered.addAll(membersFromConfig);
        return ordered.stream().findFirst().orElse("localhost:9091");
    }

    private String httpBaseForGrpc(String grpcHostPort) {
        String[] parts = grpcHostPort.split(":");
        String host = normalizeHost(parts[0]);
        int grpcPort = Integer.parseInt(parts[1]);
        int httpPort = 8081 + (grpcPort - 9091);
        return "http://" + host + ":" + httpPort;
    }

    private static String normalizeHost(String host) {
        if (host == null) return "localhost";
        return host.replace("[0:0:0:0:0:0:0:1]", "localhost")
                .replace("::1", "localhost")
                .replace("127.0.0.1", "localhost")
                .trim();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    private void httpPostNoBody(String url) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(3000))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            http.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            log.warn("POST {} failed: {}", url, e.getMessage());
        }
    }

    private void httpPost(String url, byte[] body, String contentType) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(4000))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        try {
            http.send(req, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            log.warn("POST {} failed: {}", url, e.getMessage());
        }
    }

    private String httpGet(String url) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(2500))
                .GET()
                .build();
        try {
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return resp.body();
        } catch (Exception e) {
            throw new RuntimeException("GET " + url + " failed", e);
        }
    }

    private List<String> allMembersFallback() {
        LinkedHashSet<String> all = new LinkedHashSet<>();
        all.addAll(safeList(ns.getFullMembership()));
        all.addAll(membersFromConfig);
        if (all.isEmpty() && notBlank(ns.getSelfNodeId())) all.add(ns.getSelfNodeId());
        return new ArrayList<>(all);
    }

    private static List<String> safeList(List<String> in) {
        return (in == null) ? Collections.emptyList() : in;
    }

    private static boolean notBlank(String s) { return s != null && !s.isBlank(); }

    private static List<String> parseMembers(String cfg) {
        if (cfg == null || cfg.isBlank()) {
            return List.of("localhost:9091","localhost:9092","localhost:9093","localhost:9094","localhost:9095");
        }
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String p : cfg.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) set.add(t);
        }
        return List.copyOf(set);
    }

    public void triggerElection() {
        try {
            for (String member : allMembersFallback()) {
                String url = httpBaseForGrpc(member) + "/admin/triggerElection";
                httpPostNoBody(url);
            }
            log.info("Election trigger broadcast to all nodes");
        } catch (Exception e) {
            log.error("Error triggering election broadcast: {}", e.toString());
        }
    }

    private static String normalize(String hostPort) {
        if (hostPort == null || hostPort.isBlank()) return null;
        String[] parts = hostPort.trim().split(":");
        if (parts.length != 2) return hostPort.trim();
        String host = parts[0]
                .replace("[0:0:0:0:0:0:0:1]", "localhost")
                .replace("::1", "localhost")
                .replace("127.0.0.1", "localhost")
                .trim();
        return host + ":" + parts[1].trim();
    }
}
