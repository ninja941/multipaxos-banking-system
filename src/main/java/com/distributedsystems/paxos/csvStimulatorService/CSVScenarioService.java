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
import java.util.concurrent.TimeUnit;

@Service
public class CSVScenarioService {

    private static final Logger log = LoggerFactory.getLogger(CSVScenarioService.class);

    private final NodeState ns;
    private final ClientRouter client;
    private static int clientCounter = 0;

    private static final Map<Integer, Integer> GRPC_TO_HTTP = Map.of(
            9091, 8081,
            9092, 8082,
            9093, 8083,
            9094, 8084,
            9095, 8085
    );

    public CSVScenarioService(NodeState ns, ClientRouter client) {
        this.ns = ns;
        this.client = client;
    }



    private Reader resolveCsvReader(String csvPath) {
        try {
            if (csvPath.startsWith("classpath:")) {
                String resourceName = csvPath.replace("classpath:", "").trim();
                InputStream stream = getClass().getClassLoader().getResourceAsStream(resourceName);
                if (stream == null) {
                    log.error("Classpath resource not found: {}", resourceName);
                    return null;
                }
                log.info("Loading CSV from classpath: {}", resourceName);
                return new InputStreamReader(stream, StandardCharsets.UTF_8);
            } else {
                log.info("Loading CSV from file system: {}", csvPath);
                return new FileReader(csvPath, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            log.error("Error loading CSV: {}", e.getMessage());
            return null;
        }
    }



    public String runCsvScenario(String csvPath) {
            String self = ns.getSelfNodeId();
            log.info("[{}] Running CSV scenario from {}", self, csvPath);

            try (Reader in = resolveCsvReader(csvPath)) {
                if (in == null) return "Error: file not found - " + csvPath;

                int retries = 10;
                while (ns.getLeaderId() == null && retries-- > 0) {
                    log.warn("[{}] Waiting for initial leader election...", self);
                    TimeUnit.SECONDS.sleep(1);
                }

                Iterable<CSVRecord> records = CSVFormat.DEFAULT
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim(true)
                        .parse(in);

                for (CSVRecord record : records) {
                    String setNumber = record.get("Set Number").trim();
                    String txList    = record.get("Transactions").trim();
                    String liveNodes = record.get("Live Nodes").trim();

                    Set<String> live = new HashSet<>();
                    for (String n : liveNodes.replace("[", "").replace("]", "").split(",")) {
                        n = n.trim(); if (n.isEmpty()) continue;
                        live.add("localhost:" + nodeToGrpcPort(n));
                    }

                    ns.overrideLiveNodesForCsv(live);
                    log.info("=== Running SET {} | LiveNodes={} ===", setNumber, live);

                    for (String tx : txList.split(";")) {
                        tx = tx.replace("(", "").replace(")", "").trim();
                        if (tx.isEmpty()) continue;

                        String[] parts = tx.split(",");
                        if (parts.length != 3) {
                            log.warn("Invalid tx format: {}", tx);
                            continue;
                        }

                        String from = parts[0].trim();
                        String to   = parts[1].trim();
                        long amount = Long.parseLong(parts[2].trim());
                        String clientId = "client-" + ((clientCounter++ % 10) + 1);
                        long clientTs = System.currentTimeMillis();

                        ClientTransactionResponse reply = client.submitWithRetry(
                                from,
                                to,
                                amount,
                                clientId,
                                clientTs,
                                3
                        );

                        if (!reply.issuccess()) {
                            log.error("[{}] Transaction failed (client path): {} -> {} : {} (leaderHint={})",
                                    self, from, to, amount, reply.getLeaderId());
                        }
                    }

                    ns.clearCsvLiveOverride();
                    log.info("=== Completed SET {} ===", setNumber);
                }

                return "CSV scenario completed successfully.";
            } catch (Exception e) {
                log.error("[{}] CSV scenario error: {}", ns.getSelfNodeId(), e.getMessage(), e);
                return "Error: " + e.getMessage();
            }
        }

        private int nodeToGrpcPort(String nodeName) {
            return switch (nodeName.trim().toLowerCase()) {
                case "n1" -> 9091;
                case "n2" -> 9092;
                case "n3" -> 9093;
                case "n4" -> 9094;
                case "n5" -> 9095;
                default -> throw new IllegalArgumentException("Unknown node name: " + nodeName);
            };
        }
    }




