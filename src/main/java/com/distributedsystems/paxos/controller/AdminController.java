package com.distributedsystems.paxos.controller;

import com.distributedsystems.paxos.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
public class AdminController {
    private static final Logger log = LoggerFactory.getLogger(AdminController.class);
    private final NodeState ns;

    public AdminController(NodeState ns) {
        this.ns = ns;
    }

    @PostMapping("/admin/csv/live")
    public ResponseEntity<Void> csvLive(@RequestBody Collection<String> live) {
        ns.overrideLiveNodesForCsv(live);
        log.info("[{}] CSV override: Live nodes -> {} (selfLive={})",
                ns.getSelfNodeId(), ns.getLiveNodes(), ns.isSelfLive());
        return ResponseEntity.ok().build();
    }

    @PostMapping("/admin/csv/clear")
    public ResponseEntity<Void> csvClear() {
        ns.clearCsvLiveOverride();
        return ResponseEntity.ok().build();
    }


    @PostMapping("/admin/pause")
    public ResponseEntity<Void> pause() {
        ns.pauseAllTimers();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/admin/resume")
    public ResponseEntity<Void> resume() {
        ns.resumeTimers();
        return ResponseEntity.ok().build();
    }


    @PostMapping("/admin/lf")
    public ResponseEntity<Void> lf() {
        ns.failForCurrentSet();
        return ResponseEntity.ok().build();
    }

    @PostMapping("/admin/clearLeader")
    public ResponseEntity<Void> clearLeader() {
        ns.clearLeader();
        return ResponseEntity.ok().build();
    }

    @GetMapping("/admin/whois")
    public ResponseEntity<String> whois() {
        String leader = ns.getLeaderId();
        return ResponseEntity.ok(leader == null ? "" : leader);
    }
}
