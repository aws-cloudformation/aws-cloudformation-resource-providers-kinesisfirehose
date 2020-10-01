package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.kinesisfirehose.deliverystream.HandlerUtils.HandlerType;
import java.time.Duration;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 130;
    static final String DELIVERY_STREAM_DELETED = "Delivery Stream Deleted";
    static final String TIMED_OUT_MESSAGE = "Timed out waiting for the delivery stream to get DELETED.";
    private static final int CALLBACK_DELAY_IN_SECONDS = 30;
    private FirehoseAPIWrapper firehoseAPIWrapper;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        firehoseAPIWrapper = FirehoseAPIWrapper.builder().firehoseClient(FirehoseClient.create()).clientProxy(proxy).build();

        logger.log(String.format("Delete Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));

        final CallbackContext currentContext = callbackContext == null
                ? CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build()
                : callbackContext;

        if (callbackContext == null && !HandlerUtils.doesDeliveryStreamExistWithName(model.getDeliveryStreamName(),
            firehoseAPIWrapper)) {
            final Exception e = ResourceNotFoundException.builder()
                    .message("Firehose doesn't exist with the name: " + model.getDeliveryStreamName())
                    .build();
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.DELETE));
        }

        // This Lambda will continually be re-invoked with the current state of the instance, finally succeeding when state stabilizes.
        return deleteDeliveryStreamAndUpdateProgress(model, currentContext, logger);
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDeliveryStreamAndUpdateProgress(final ResourceModel model,
                                                                                                final CallbackContext callbackContext,
                                                                                                final Logger logger) {
        val deliveryStreamStatus = callbackContext.getDeliveryStreamStatus();
        logger.log("deliveryStreamStatus = " + deliveryStreamStatus);

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        int stabilizationRetriesRemaining = NUMBER_OF_STATUS_POLL_RETRIES;
        final boolean allowForceDelete = true;
        if (deliveryStreamStatus == null) {
            try {
                firehoseAPIWrapper.deleteDeliveryStream(model.getDeliveryStreamName(), allowForceDelete);
            } catch (final Exception e) {
                logger.log(String.format("deleteDeliveryStream failed with exception %s", e.getMessage()));
                return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.DELETE));
            }
        } else {
            stabilizationRetriesRemaining = callbackContext.getStabilizationRetriesRemaining() - 1;
        }

        val currentDeliveryStreamStatus = getDeliveryStreamStatus(firehoseAPIWrapper, model);
        if (currentDeliveryStreamStatus.equals(DELIVERY_STREAM_DELETED)) {
            return ProgressEvent.defaultSuccessHandler(null);
        } else {
            return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                            .deliveryStreamStatus(currentDeliveryStreamStatus)
                            .stabilizationRetriesRemaining(stabilizationRetriesRemaining)
                            .build(),
                    (int) Duration.ofSeconds(CALLBACK_DELAY_IN_SECONDS).getSeconds(),
                    model);
        }
    }

    private String getDeliveryStreamStatus(final FirehoseAPIWrapper firehoseAPIWrapper, final ResourceModel model) {
        try {
            return firehoseAPIWrapper.describeDeliveryStream(model.getDeliveryStreamName()).deliveryStreamDescription().deliveryStreamStatusAsString();
        } catch (ResourceNotFoundException e) {
            //Delivery Stream got successfully deleted.
            return DELIVERY_STREAM_DELETED;
        }
    }
}
