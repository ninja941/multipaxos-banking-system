package com.distributedsystems.paxos.service;

import com.distributedsystems.paxos.proto.Paxos.AcceptRequest;
import com.distributedsystems.paxos.proto.Paxos.AcceptedReply;
import com.distributedsystems.paxos.proto.Paxos.CommitReply;
import com.distributedsystems.paxos.proto.Paxos.CommitRequest;
import com.distributedsystems.paxos.proto.Paxos.HeartbeatReply;
import com.distributedsystems.paxos.proto.Paxos.HeartbeatRequest;
import com.distributedsystems.paxos.proto.Paxos.NewViewReply;
import com.distributedsystems.paxos.proto.Paxos.NewViewRequest;
import com.distributedsystems.paxos.proto.Paxos.PrepareRequest;
import com.distributedsystems.paxos.proto.Paxos.PromiseReply;
import com.distributedsystems.paxos.proto.Paxos.SyncReply;
import com.distributedsystems.paxos.proto.Paxos.SyncRequest;
import com.distributedsystems.paxos.proto.PaxosServiceGrpc;

import com.distributedsystems.paxos.service.phaseServiceImpl.AcceptPhaseServiceImpl;
import com.distributedsystems.paxos.service.phaseServiceImpl.CommitPhaseServiceImpl;
import com.distributedsystems.paxos.service.scheduler.HeartbeatServiceImpl;
import com.distributedsystems.paxos.service.phaseServiceImpl.NewViewPhaseServiceImpl;
import com.distributedsystems.paxos.service.phaseServiceImpl.PreparePhaseServiceImpl;

import com.distributedsystems.paxos.state.NodeState;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class PaxosPhaseServiceImpl extends PaxosServiceGrpc.PaxosServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(PaxosPhaseServiceImpl.class);

    private final NodeState nodeState;
    private final PreparePhaseServiceImpl prepare;
    private final NewViewPhaseServiceImpl newView;
    private final AcceptPhaseServiceImpl accept;
    private final CommitPhaseServiceImpl commit;
    private final HeartbeatServiceImpl heartbeat;

    public PaxosPhaseServiceImpl(
            NodeState nodeState,
            PreparePhaseServiceImpl prepare,
            NewViewPhaseServiceImpl newView,
            AcceptPhaseServiceImpl accept,
            CommitPhaseServiceImpl commit,
            HeartbeatServiceImpl heartbeat
    ) {
        this.nodeState = nodeState;
        this.prepare = prepare;
        this.newView = newView;
        this.accept = accept;
        this.commit = commit;
        this.heartbeat = heartbeat;
    }

    @Override
    public void prepare(PrepareRequest request, StreamObserver<PromiseReply> responseObserver) {
        try {
            responseObserver.onNext(prepare.onPrepare(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] prepare() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void newView(NewViewRequest request, StreamObserver<NewViewReply> responseObserver) {
        try {
            responseObserver.onNext(newView.onNewView(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] newView() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void accept(AcceptRequest request, StreamObserver<AcceptedReply> responseObserver) {
        try {
            responseObserver.onNext(accept.onAccept(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] accept() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        try {
            responseObserver.onNext(commit.onCommit(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] commit() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatReply> responseObserver) {
        try {
            responseObserver.onNext(heartbeat.onHeartbeat(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] heartbeat() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void sync(SyncRequest request, StreamObserver<SyncReply> responseObserver) {
        try {
            responseObserver.onNext(heartbeat.onSync(request));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("[{}] sync() failed", nodeState.getSelfNodeId(), e);
            responseObserver.onError(e);
        }
    }
}
