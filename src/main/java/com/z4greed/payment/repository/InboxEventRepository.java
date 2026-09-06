package com.z4greed.payment.repository;

import com.z4greed.payment.entity.InboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InboxEventRepository extends JpaRepository<InboxEventEntity, String> {}
