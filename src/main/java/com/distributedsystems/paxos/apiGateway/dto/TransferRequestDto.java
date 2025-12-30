package com.distributedsystems.paxos.apiGateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TransferRequestDto {
    @NotBlank public String fromAccountId;
    @NotBlank public String toAccountId;
    @NotNull @Min(1) public Long amountCents;

    @NotBlank public String clientId;
    @NotNull public Long clientTsMillis;
}
