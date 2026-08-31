package com.z4greed.payment.repository;

import com.z4greed.payment.entity.PaymentAttemptEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentAttemptRepository extends JpaRepository<PaymentAttemptEntity, Long> {}
