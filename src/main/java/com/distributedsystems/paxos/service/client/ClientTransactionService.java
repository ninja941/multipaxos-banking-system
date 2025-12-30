package com.distributedsystems.paxos.service.client;

import com.distributedsystems.paxos.Repository.TransactionBlockRepository;
import com.distributedsystems.paxos.model.TransactionBlock;
import com.distributedsystems.paxos.apiGateway.dto.TransferRequestDto;
import com.distributedsystems.paxos.proto.Paxos.*;
import com.distributedsystems.paxos.proto.PaxosServiceGrpc;
import com.distributedsystems.paxos.service.phaseServiceImpl.AcceptPhaseServiceImpl;
import com.distributedsystems.paxos.service.phaseServiceImpl.CommitPhaseServiceImpl;
import com.distributedsystems.paxos.state.NodeState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class ClientTransactionService {

    private static final Logger log = LoggerFactory.getLogger(ClientTransactionService.class);

    private final NodeState ns;
    private final TransactionBlockRepository txRepo;
    private final RestTemplate http = new RestTemplate();
    private final AcceptPhaseServiceImpl acceptSvc;
    private final CommitPhaseServiceImpl commitSvc;

    public ClientTransactionService(NodeState ns,
                                    TransactionBlockRepository txRepo,
                                    AcceptPhaseServiceImpl acceptSvc,
                                    CommitPhaseServiceImpl commitSvc) {
        this.ns = ns;
        this.txRepo = txRepo;
        this.acceptSvc = acceptSvc;
        this.commitSvc = commitSvc;
    }

    public record CommitResult(long seq, String leaderId, boolean committed) {}

    public CommitResult proposeTransfer(TransferRequestDto dto) {
        Optional<TransactionBlock> already = txRepo.findByClientIdAndClientTsMillis(dto.clientId, dto.clientTsMillis);
        if (already.isPresent()) {
            TransactionBlock tb = already.get();
            return new CommitResult(tb.getSequence(), ns.getLeaderId(), true);
        }

        if (!ns.isLeader()) {
            String leader = ns.getLeaderId();
            if (leader != null && !leader.isBlank()) {
                try {
                    String url = grpcToHttp(leader) + "/api/transfer";
                    ResponseEntity<CommitResult> resp = http.postForEntity(url, dto, CommitResult.class);
                    if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                        return resp.getBody();
                    }
                } catch (Exception e) {
                    log.warn("[{}] Forward to leader {} failed: {}", ns.getSelfNodeId(), leader, e.toString());
                }
            }
            return new CommitResult(-1L, leader, false);
        }

        if (!ns.isSelfLive()) {
            log.warn("[{}] Leader but NOT live for current set – refusing client op.", ns.getSelfNodeId());
            return new CommitResult(-1L, ns.getLeaderId(), false);
        }

        long ballot = ns.getHighestBallotSeen();
        long seq = Math.max(ns.nextProposalSeq(), durableMaxSeq() + 1);

        ClientRequest value = ClientRequest.newBuilder()
                .setClientId(dto.clientId)
                .setTimestamp(dto.clientTsMillis)
                .setValue(ClientRequestValue.newBuilder()
                        .setTransfer(Transfer.newBuilder()
                                .setFrom(dto.fromAccountId)
                                .setTo(dto.toAccountId)
                                .setAmount(dto.amountCents)))
                .build();

        AcceptEntry entry = AcceptEntry.newBuilder()
                .setBallot(Ballot.newBuilder()
                        .setNumber(ballot)
                        .setProposer(NodeId.newBuilder().setId(ns.getSelfNodeId())))
                .setSequence(Sequence.newBuilder().setValue(seq))
                .setRequest(value)
                .build();

        try {
            acceptSvc.onAccept(AcceptRequest.newBuilder()
                    .setFrom(NodeId.newBuilder().setId(ns.getSelfNodeId()))
                    .setEntry(entry)
                    .build());
        } catch (Exception e) {
            log.error("[{}] self-ACCEPT failed for seq={}: {}", ns.getSelfNodeId(), seq, e.toString(), e);
            return new CommitResult(seq, ns.getLeaderId(), false);
        }

        int acks = 1;
        for (String peer : ns.livePeers()) {
            try {
                AcceptedReply r = stub(peer).accept(
                        AcceptRequest.newBuilder()
                                .setFrom(NodeId.newBuilder().setId(ns.getSelfNodeId()))
                                .setEntry(entry)
                                .build());
                if (r.getOk()) acks++;
            } catch (StatusRuntimeException e) {
                log.debug("[{}] ACCEPT to {} failed: {}", ns.getSelfNodeId(), peer, e.getStatus());
            }
        }

        if (acks < ns.quorum()) {
            log.warn("[{}] No LIVE majority for seq={} (acks={}/liveN={})",
                    ns.getSelfNodeId(), seq, acks, ns.getLiveNodes().size());
            return new CommitResult(seq, ns.getLeaderId(), false);
        }

        CommitRequest commitReq = CommitRequest.newBuilder()
                .setFrom(NodeId.newBuilder().setId(ns.getSelfNodeId()))
                .setEntry(entry)
                .build();
        try {
            commitSvc.onCommit(commitReq);
        } catch (Exception e) {
            log.error("[{}] local onCommit failed for seq={}: {}", ns.getSelfNodeId(), seq, e.toString(), e);
            return new CommitResult(seq, ns.getLeaderId(), false);
        }

        for (String peer : ns.livePeers()) {
            try {
                stub(peer).withDeadlineAfter(1000, TimeUnit.MILLISECONDS).commit(commitReq);
            } catch (StatusRuntimeException ignored) {}
        }

        return new CommitResult(seq, ns.getLeaderId(), true);
    }

    private PaxosServiceGrpc.PaxosServiceBlockingStub stub(String target) {
        String[] p = target.split(":");
        ManagedChannel ch = ManagedChannelBuilder.forAddress(p[0], Integer.parseInt(p[1]))
                .usePlaintext().build();
        return PaxosServiceGrpc.newBlockingStub(ch).withDeadlineAfter(2, TimeUnit.SECONDS);
    }

    private String grpcToHttp(String leader) {
        String[] p = leader.split(":");
        int grpc = Integer.parseInt(p[1]);
        int http = 8000 + (grpc - 9090);
        return "http://" + p[0] + ":" + http;
    }

    private long durableMaxSeq() {
        try {
            var all = txRepo.findAllByOrderBySequenceAsc();
            if (all == null || all.isEmpty()) return 0L;
            return all.get(all.size() - 1).getSequence();
        } catch (Exception e) {
            return 0L;
        }
    }
}
