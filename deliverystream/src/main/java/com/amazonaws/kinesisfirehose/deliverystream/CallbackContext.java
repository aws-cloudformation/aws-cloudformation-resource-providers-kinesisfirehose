package com.amazonaws.kinesisfirehose.deliverystream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CallbackContext {
	private Integer stabilizationRetriesRemaining;
	private String deliveryStreamStatus;
}
