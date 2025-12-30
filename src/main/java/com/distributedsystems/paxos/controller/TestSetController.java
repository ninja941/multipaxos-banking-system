package com.distributedsystems.paxos.controller;

import com.distributedsystems.paxos.model.CustomerAccount;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.Repository.CustomerAccountRepository;
import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.proto.Paxos;
import com.distributedsystems.paxos.service.phaseServiceImpl.LeaderElectionImpl;
import com.distributedsystems.paxos.state.NodeState;
import com.distributedsystems.paxos.csvStimulatorService.CSVScenarioService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class TestSetController {

    private final CSVScenarioService csvService;
    private final CustomerAccountRepository accountRepo;
    private final TransactionBlockRepository txRepo;
    private final NodeState nodeState;
    private final LeaderElectionImpl leader;


    @PostMapping("/runCSV")
    public String runCsv(@RequestBody String csvContent) {
        csvService.runCsvScenario(csvContent);
        return "CSV scenario started. Check logs for execution.";
    }


    @GetMapping("/printDB")
    public List<Map<String, Object>> printDB() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (CustomerAccount a : accountRepo.findAll()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", a.getName());
            row.put("balance", a.getBalance());
            out.add(row);
        }
        return out;
    }

    @GetMapping("/printLog")
    public List<Map<String, Object>> printLog() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (TransactionBlock t : txRepo.findAllByOrderBySequenceAsc()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("seq", t.getSequence());
            row.put("from", t.getFromAccountId());
            row.put("to", t.getToAccountId());
            row.put("amount", t.getAmountCents());
            row.put("committedAt", t.getCommittedAt());
            row.put("executedAt", t.getExecutedAt());
            out.add(row);
        }
        return out;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("self", nodeState.getSelfNodeId());
        r.put("leader", nodeState.getLeaderId());
        r.put("highestBallotSeen", nodeState.getHighestBallotSeen());
        r.put("lastCommittedSeq", nodeState.getLastCommittedSeq());
        return r;
    }

    @GetMapping("/printStatus/{seq}")
    public String printStatusLocal(@PathVariable long seq) {
        var opt = txRepo.findBySequence(seq);
        String s = opt.map(tb -> (tb.getExecutedAt()!=null) ? "E"
                        : (tb.getCommittedAt()!=null) ? "C"
                        : (tb.getBallot()!=null) ? "A" : "X")
                .orElse("X");
        return "Node " + nodeState.getSelfNodeId() + " → seq " + seq + " : " + s;
    }
    @GetMapping("/view")
    public String printView() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== NEW-VIEW history @ ")
                .append(nodeState.getSelfNodeId())
                .append(" ===\n\n");

        for (var v : nodeState.getNewViewHistory()) {
            String ts = new java.util.Date((Long) v.get("ts")).toString();
            sb.append(String.format("Time: %s | Leader: %s | Ballot: %s | Entries: %s\n",
                    ts, v.get("leader"), v.get("ballot"), v.get("entries")));

            @SuppressWarnings("unchecked")
            var entries = (List<Paxos.AcceptEntry>) v.get("entryList");
            if (entries != null) {
                for (var e : entries) {
                    var t = e.getRequest().getValue().getTransfer();
                    sb.append(String.format("  seq=%d  %s→%s  amt=%d  client=%s  ts=%d\n",
                            e.getSequence().getValue(),
                            t.getFrom(), t.getTo(),
                            t.getAmount(),
                            e.getRequest().getClientId(),
                            e.getRequest().getTimestamp()));
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }

}



