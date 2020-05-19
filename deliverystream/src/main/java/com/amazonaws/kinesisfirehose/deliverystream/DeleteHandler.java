package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.util.StringUtils;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeleteDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 130;
    static final String DELIVERY_STREAM_DELETED = "Delivery Stream Deleted";
    static final String TIMED_OUT_MESSAGE = "Timed out waiting for the delivery stream to get DELETED.";

    private AmazonWebServicesClientProxy clientProxy;
    private final FirehoseClient firehoseClient = FirehoseClient.create();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        clientProxy = proxy;

        logger.log(String.format("Delete Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));

        final CallbackContext currentContext = callbackContext == null
                ? CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build()
                : callbackContext;

        if(callbackContext == null && !HandlerUtils.doesDeliveryStreamExistWithName(model,
                clientProxy, firehoseClient)) {
            final Exception e = ResourceNotFoundException.builder()
                    .message("Firehose doesn't exist with the name: " + model.getDeliveryStreamName())
                    .build();
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        // This Lambda will continually be re-invoked with the current state of the instance, finally succeeding when state stabilizes.
        return deleteDeliveryStreamAndUpdateProgress(model, currentContext, logger);
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteDeliveryStreamAndUpdateProgress(ResourceModel model,
                                                                                                CallbackContext callbackContext,
                                                                                                final Logger logger) {
        val deliveryStreamStatus = callbackContext.getDeliveryStreamStatus();
        logger.log("deliveryStreamStatus = " + deliveryStreamStatus);

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        int stabilizationRetriesRemaining = NUMBER_OF_STATUS_POLL_RETRIES;
        if (deliveryStreamStatus == null) {
            try {
                deleteDeliveryStream(model);
            } catch (final Exception e) {
                logger.log(String.format("deleteDeliveryStream failed with exception %s", e.getMessage()));
                return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
            }
        } else {
            stabilizationRetriesRemaining = callbackContext.getStabilizationRetriesRemaining() - 1;
        }

        val currentDeliveryStreamStatus = getDeliveryStreamStatus(model);
        if (currentDeliveryStreamStatus.equals(DELIVERY_STREAM_DELETED)) {
            return ProgressEvent.defaultSuccessHandler(model);
        } else {
            return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                            .deliveryStreamStatus(currentDeliveryStreamStatus)
                            .stabilizationRetriesRemaining(stabilizationRetriesRemaining)
                            .build(),
                    (int) Duration.ofSeconds(30).getSeconds(),
                    model);
        }
    }

    private void deleteDeliveryStream(ResourceModel model) {
        val deleteDeliveryStreamRequest = DeleteDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .allowForceDelete(true)
                .build();

        clientProxy.injectCredentialsAndInvokeV2(deleteDeliveryStreamRequest, firehoseClient::deleteDeliveryStream);
    }

    private String getDeliveryStreamStatus(ResourceModel model) {
        try {
            val response = clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                            .deliveryStreamName(model.getDeliveryStreamName())
                            .build(),
                    firehoseClient::describeDeliveryStream);
            return response.deliveryStreamDescription().deliveryStreamStatusAsString();
        } catch (ResourceNotFoundException e) {
            //Delivery Stream got successfully deleted.
            return DELIVERY_STREAM_DELETED;
        }
    }
}
