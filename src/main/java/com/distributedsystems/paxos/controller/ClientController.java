package com.distributedsystems.paxos.controller;

import com.distributedsystems.paxos.apiGateway.dto.ClientTransactionResponse;
import com.distributedsystems.paxos.apiGateway.dto.ClientTransactionRequest;
import com.distributedsystems.paxos.service.phaseServiceImpl.LeaderElectionImpl;
import com.distributedsystems.paxos.state.NodeState;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/client")
public class ClientController {
    private static final Logger log = LoggerFactory.getLogger(ClientController.class);

    private final NodeState ns;
    private final LeaderElectionImpl leader;

    private static final Map<Integer, Integer> GRPC_TO_HTTP = Map.of(
            9091, 8081,
            9092, 8082,
            9093, 8083,
            9094, 8084,
            9095, 8085
    );

    private final ObjectMapper mapper = new ObjectMapper();

    public ClientController(NodeState ns, LeaderElectionImpl leader) {
        this.ns = ns;
        this.leader = leader;
    }

    @PostMapping(
            value = "/submit",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ClientTransactionResponse submit(@RequestBody ClientTransactionRequest req) {
        final String self = ns.getSelfNodeId();

        if (req.getClientId() == null) req.setClientId("cli-" + UUID.randomUUID());
        if (req.getClientTsMillis() == null) req.setClientTsMillis(System.currentTimeMillis());




        if (ns.isLeader()) {
            boolean success = leader.proposeTransaction(req.getFrom(), req.getTo(), req.getAmount());
            if (success) {
                return ClientTransactionResponse.check(
                        ns.getLeaderId(),
                        ns.getHighestBallotSeen(),
                        null,
                        "accepted@" + self
                );
            } else {
                return ClientTransactionResponse.fail(ns.getLeaderId(), "proposal failed@" + self);
            }
        }

        // Not leader → forward to known leader
        final String leaderId = ns.getLeaderId();
        if (leaderId == null || leaderId.isBlank()) {
            log.warn("[{}] submit: no leader known; returning hint-only", self);
            // If you have ns.getLastKnownLeaderId(), use that as a hint; else null
            String hint = safe(ns.getLeaderId()); // or ns.getLastKnownLeaderId() if available
            return ClientTransactionResponse.fail(hint, "no leader known@" + self);
        }

        try {
            int leaderGrpcPort = Integer.parseInt(leaderId.split(":")[1]);
            int leaderHttpPort = GRPC_TO_HTTP.getOrDefault(leaderGrpcPort, 8081);
            URL url = new URL("http://localhost:" + leaderHttpPort + "/api/client/submit");

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(1500);
            conn.setReadTimeout(4000);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");

            byte[] body = mapper.writeValueAsBytes(req);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
            String resp = readAll(is);
            conn.disconnect();

            ClientTransactionResponse reply = mapper.readValue(resp, ClientTransactionResponse.class);

            if (reply.getLeaderId() == null || reply.getLeaderId().isBlank()) {
                reply.setLeaderId(leaderId);
            }
            return reply;

        } catch (Exception e) {
            log.error("[{}] forward-to-leader {} failed: {}", self, leaderId, e.toString());
            return ClientTransactionResponse.fail(leaderId, "forward failed@" + self + ": " + e.getMessage());
        }
    }

    private static String readAll(InputStream in) throws IOException {
        if (in == null) return "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            for (String line; (line = br.readLine()) != null; ) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private static String safe(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
