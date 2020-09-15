package com.amazonaws.kinesisfirehose.deliverystream;

import java.time.Duration;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.InvalidArgumentException;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    private AmazonWebServicesClientProxy clientProxy;
    private final FirehoseClient firehoseClient = FirehoseClient.create();
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 20;
    static final String TIMED_OUT_MESSAGE = "Timed out waiting for the delivery stream Update handler to stabilize";
    static final String ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT = "Unable to %s delivery stream encryption";

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        clientProxy = proxy;

        logger.log(String.format("Update Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));
        val currentContext = callbackContext != null
            ? callbackContext : CallbackContext.builder()
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();
        return updateDeliveryStreamAndUpdateProgress(model, currentContext, logger);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDeliveryStreamAndUpdateProgress(ResourceModel model,
        CallbackContext callbackContext,
        final Logger logger) {
        val deliveryStreamEncryptionStatus = callbackContext.getDeliveryStreamEncryptionStatus();
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }
        DescribeDeliveryStreamResponse describeDeliveryStreamResp;
        try {
            describeDeliveryStreamResp = getDescribeDeliveryStreamResponse(model);
        }catch (final Exception e) {
            logger.log(String.format("DescribeDeliveryStream failed with exception %s", e.getMessage()));
            // In case describe fails(either on the first call or on the callbacks) we would set the
            // previous values of callbackContext, return and mark handler status as in-progress for cfn to retry.
            return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                    .deliveryStreamStatus(callbackContext.getDeliveryStreamStatus())
                    .deliveryStreamEncryptionStatus(callbackContext.getDeliveryStreamEncryptionStatus())
                    .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                    .build(),
                (int) Duration.ofSeconds(30).getSeconds(),
                model);
        }

        // In case of callbacks.
        if(deliveryStreamEncryptionStatus != null) {
            val currentDSEncryptionStatus = describeDeliveryStreamResp.deliveryStreamDescription().deliveryStreamEncryptionConfiguration().statusAsString();
            if (currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.ENABLED.toString())
                || currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.DISABLED.toString()))
                return ProgressEvent.defaultSuccessHandler(model);
            else if(currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.ENABLING_FAILED.toString())
                || currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.DISABLING_FAILED.toString())) {
                val errMsg = getErrorMessageFromEncryptionStatus(currentDSEncryptionStatus);
                Exception exp = InvalidArgumentException.builder()
                    .message(errMsg).build();
                return ProgressEvent.defaultFailureHandler(exp, ExceptionMapper.mapToHandlerErrorCode(exp));
            } else {
                return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                        .deliveryStreamStatus(describeDeliveryStreamResp.deliveryStreamDescription().deliveryStreamStatusAsString())
                        .deliveryStreamEncryptionStatus(currentDSEncryptionStatus)
                        .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                        .build(),
                    (int) Duration.ofSeconds(30).getSeconds(),
                    model);
            }
        }

        try {
            updateDestination(model, logger, describeDeliveryStreamResp);
        }catch (final Exception e) {
            logger.log(String.format("UpdateDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        EncryptionAction encryptionAction = getEncryptionActionToPerform(
            model, describeDeliveryStreamResp);
        try {
            updateEncryptionOnDeliveryStream(model, logger, encryptionAction);
        }catch (final Exception e) {
            logger.log(String.format("updateEncryptionOnDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        // If no encryption action was performed, mark this as success as per existing flow, no need to callback.
        if (encryptionAction == EncryptionAction.DO_NOTHING)
            return ProgressEvent.defaultSuccessHandler(model);
        // If the delivery stream encryption was either Started or stopped, it is supposed to have a status.
        val describeResp = getDescribeDeliveryStreamResponse(model);
        return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                .deliveryStreamStatus(describeResp.deliveryStreamDescription().deliveryStreamStatusAsString())
                .deliveryStreamEncryptionStatus(describeResp.deliveryStreamDescription().deliveryStreamEncryptionConfiguration().statusAsString())
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build(),
            (int) Duration.ofSeconds(30).getSeconds(),
            model);
    }

    private String getErrorMessageFromEncryptionStatus(String deliveryStreamEncryptionStatus){
        if (DeliveryStreamEncryptionStatus.ENABLING_FAILED.toString().equals(deliveryStreamEncryptionStatus)){
            return String.format(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "start");
        }
        return String.format(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "stop");
    }

    private EncryptionAction getEncryptionActionToPerform(ResourceModel model,
        DescribeDeliveryStreamResponse describeResponse) {
        EncryptionAction encryptionAction = EncryptionAction.DO_NOTHING;
        val deliveryStreamEncryptionConfig = model.getDeliveryStreamEncryptionConfigurationInput();
        val existingDeliveryStreamEncryptionConfig = describeResponse.deliveryStreamDescription().deliveryStreamEncryptionConfiguration();
        if (deliveryStreamEncryptionConfig != null) {
            encryptionAction = EncryptionAction.START;
        }
        else if (existingDeliveryStreamEncryptionConfig != null && !existingDeliveryStreamEncryptionConfig.statusAsString().equals(DeliveryStreamEncryptionStatus.DISABLED.toString())){
            encryptionAction = EncryptionAction.STOP;
        }
        return encryptionAction;
    }

    public enum EncryptionAction {
        DO_NOTHING("DO_NOTHING"),
        START("START"),
        STOP("STOP");
        private final String value;

        private EncryptionAction(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }


    private void updateEncryptionOnDeliveryStream(
        ResourceModel model,final Logger logger, EncryptionAction encryptionAction) {
        switch (encryptionAction){
            case DO_NOTHING:
                break;
            case START:
                val startEncryptionRequest = StartDeliveryStreamEncryptionRequest.builder()
                    .deliveryStreamName(model.getDeliveryStreamName())
                    .deliveryStreamEncryptionConfigurationInput(HandlerUtils.translateDeliveryStreamEncryptionConfigurationInput(model.getDeliveryStreamEncryptionConfigurationInput()))
                    .build();
                clientProxy.injectCredentialsAndInvokeV2(startEncryptionRequest, firehoseClient::startDeliveryStreamEncryption);
                break;
            case STOP:
                val stopEncryptionRequest = StopDeliveryStreamEncryptionRequest.builder()
                    .deliveryStreamName(model.getDeliveryStreamName())
                    .build();
                clientProxy.injectCredentialsAndInvokeV2(stopEncryptionRequest, firehoseClient::stopDeliveryStreamEncryption);
                break;
            default:
                logger.log(String.format("Action '%s' doesn't map to any of the available EncryptionAction.", encryptionAction
                    .toString()));
                break;
        }
    }

    private void updateDestination(
        ResourceModel model, final Logger logger, DescribeDeliveryStreamResponse describeResponse) {
        val updateDestinationRequest = UpdateDestinationRequest.builder()
            .deliveryStreamName(model.getDeliveryStreamName())
            .currentDeliveryStreamVersionId(describeResponse.deliveryStreamDescription().versionId())
            .destinationId(describeResponse.deliveryStreamDescription().destinations().get(0).destinationId())
            .s3DestinationUpdate(HandlerUtils.translateS3DestinationUpdate(model.getS3DestinationConfiguration()))
            .extendedS3DestinationUpdate(HandlerUtils.translateExtendedS3DestinationUpdate(model.getExtendedS3DestinationConfiguration()))
            .redshiftDestinationUpdate(HandlerUtils.translateRedshiftDestinationUpdate(model.getRedshiftDestinationConfiguration()))
            .elasticsearchDestinationUpdate(HandlerUtils.translateElasticsearchDestinationUpdate(model.getElasticsearchDestinationConfiguration()))
            .splunkDestinationUpdate(HandlerUtils.translateSplunkDestinationUpdate(model.getSplunkDestinationConfiguration()))
            .httpEndpointDestinationUpdate(HandlerUtils.translateHttpEndpointDestinationUpdate(model.getHttpEndpointDestinationConfiguration()))
            .build();
        clientProxy.injectCredentialsAndInvokeV2(updateDestinationRequest, firehoseClient::updateDestination);
    }

    private DescribeDeliveryStreamResponse getDescribeDeliveryStreamResponse(ResourceModel model) {
        return clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .build(),
            firehoseClient::describeDeliveryStream);
    }
}
