package com.distributedsystems.paxos.controller;
import com.distributedsystems.paxos.csvStimulatorService.CSVScenarioConcurrentService;
import com.distributedsystems.paxos.csvStimulatorService.CSVScenarioService;
import com.distributedsystems.paxos.state.NodeState;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class CSVController {
    private final CSVScenarioConcurrentService concurrent;
    private final CSVScenarioService sequential;   // your existing one
    private final NodeState nodeState;

    public CSVController(CSVScenarioConcurrentService concurrent,
                         CSVScenarioService sequential,
                         NodeState nodeState) {
        this.concurrent = concurrent;
        this.sequential = sequential;
        this.nodeState = nodeState;
    }

    @PostMapping("/run-csv")
    public String runCsv(@RequestBody String csvPath,
                         @RequestParam(defaultValue = "10") int clients) {
        if (clients <= 1) return sequential.runCsvScenario(csvPath);
        return concurrent.runCsvScenarioConcurrent(csvPath, clients);
    }
}
