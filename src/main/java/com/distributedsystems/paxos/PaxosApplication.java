package com.distributedsystems.paxos;

import com.distributedsystems.paxos.state.NodeState;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class PaxosApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaxosApplication.class, args);
    }


    @Bean
    public CommandLineRunner initNodeState(NodeState ns,
                                           @Value("${paxos.self}") String selfNode) {
        return args -> {
            ns.initSelf(selfNode);
        };
    }
}
