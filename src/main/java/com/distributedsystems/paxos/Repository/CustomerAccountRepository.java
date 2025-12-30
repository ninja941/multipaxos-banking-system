package com.distributedsystems.paxos.Repository;
import com.distributedsystems.paxos.model.CustomerAccount;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;



public interface CustomerAccountRepository extends JpaRepository<CustomerAccount, Long> {
    Optional<CustomerAccount> findByName(String name);

}
