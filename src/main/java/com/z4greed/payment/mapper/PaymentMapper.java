package com.z4greed.payment.mapper;

import com.z4greed.payment.dto.PaymentCreateDto;
import com.z4greed.payment.entity.PaymentEntity;
import org.mapstruct.*;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface PaymentMapper {
  @Named("PaymentMapper.toEntity")
  @Mapping(target = "id", ignore = true)
  @Mapping(target = "updatedAt", ignore = true)
  PaymentEntity toEntity(PaymentCreateDto paymentCreateDto);
}
