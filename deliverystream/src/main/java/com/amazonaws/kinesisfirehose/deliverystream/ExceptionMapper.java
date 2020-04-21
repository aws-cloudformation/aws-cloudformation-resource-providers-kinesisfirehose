package com.amazonaws.kinesisfirehose.deliverystream;

import lombok.Builder;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.InvalidArgumentException;
import software.amazon.awssdk.services.firehose.model.InvalidKmsResourceException;
import software.amazon.awssdk.services.firehose.model.LimitExceededException;
import software.amazon.awssdk.services.firehose.model.ResourceInUseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;

@Builder
public final class ExceptionMapper {
	private ExceptionMapper() {
	}

	/**
	 * Translates the Network Manager's client exception to a Cfn Handler Error Code.
	 * Ref: https://w.amazon.com/bin/view/AWS21/Design/Uluru/HandlerContract
	 */
	 static HandlerErrorCode mapToHandlerErrorCode(final Exception exception) {
		if (exception instanceof ResourceInUseException) {
			return HandlerErrorCode.ResourceConflict;
		} else if (exception instanceof InvalidArgumentException) {
			return HandlerErrorCode.InvalidRequest;
		} else if (exception instanceof InvalidKmsResourceException) {
			return HandlerErrorCode.InvalidRequest;
		} else if (exception instanceof LimitExceededException) {
			return HandlerErrorCode.ServiceLimitExceeded;
		} else if (exception instanceof ResourceNotFoundException) {
			return HandlerErrorCode.NotFound;
		} else if (exception instanceof FirehoseException) {
			return HandlerErrorCode.ServiceInternalError;
		} else {
			return HandlerErrorCode.InternalFailure;
		}
	}
}
