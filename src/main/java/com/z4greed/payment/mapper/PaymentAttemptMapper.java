package com.z4greed.payment.mapper;

import com.z4greed.payment.dto.PaymentAttemptCreateDto;
import com.z4greed.payment.entity.PaymentAttemptEntity;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface PaymentAttemptMapper {
  @Named("PaymentAttemptMapper.toEntity")
  @Mapping(target = "id", ignore = true)
  PaymentAttemptEntity toEntity(PaymentAttemptCreateDto paymentAttemptCreateDto);
}
