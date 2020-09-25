package com.amazonaws.kinesisfirehose.deliverystream;

import java.time.Duration;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.InvalidArgumentException;
import software.amazon.awssdk.services.firehose.model.KeyType;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    private FirehoseAPIWrapper firehoseAPIWrapper;
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 20;
    static final String TIMED_OUT_MESSAGE = "Timed out waiting for the delivery stream Update handler to stabilize";
    static final String ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT = "Unable to %s delivery stream encryption";
    private static final int CALLBACK_DELAY_IN_SECONDS = 30;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        firehoseAPIWrapper = FirehoseAPIWrapper.builder().firehoseClient(FirehoseClient.create()).clientProxy(proxy).build();

        logger.log(String.format("Update Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));
        val currentContext = callbackContext != null
            ? callbackContext : CallbackContext.builder()
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();
        return updateDeliveryStreamAndUpdateProgress(model, currentContext, logger);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDeliveryStreamAndUpdateProgress(
        ResourceModel model, CallbackContext callbackContext, final Logger logger) {
        val deliveryStreamEncryptionStatus = callbackContext.getDeliveryStreamEncryptionStatus();
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }
        DescribeDeliveryStreamResponse describeDeliveryStreamResp;
        try {
            describeDeliveryStreamResp = firehoseAPIWrapper.describeDeliveryStream(model.getDeliveryStreamName());
        } catch (ResourceNotFoundException e) {
            logger.log(String.format("DescribeDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        } catch (final Exception e) {
            logger.log(String.format("DescribeDeliveryStream failed with exception %s", e.getMessage()));
            // In case describe fails(either on the first call or on the callbacks) we would set the
            // previous values of callbackContext, return and mark handler status as in-progress for cfn to retry.
            return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                    .deliveryStreamStatus(callbackContext.getDeliveryStreamStatus())
                    .deliveryStreamEncryptionStatus(callbackContext.getDeliveryStreamEncryptionStatus())
                    .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                    .build(),
                (int) Duration.ofSeconds(CALLBACK_DELAY_IN_SECONDS).getSeconds(),
                model);
        }

        // In case of callbacks.
        if (deliveryStreamEncryptionStatus != null) {
            val currentDSEncryptionStatus = describeDeliveryStreamResp.deliveryStreamDescription().deliveryStreamEncryptionConfiguration().statusAsString();
            if (currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.ENABLED.toString())
                || currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.DISABLED.toString())) {
                return ProgressEvent.defaultSuccessHandler(model);
            }
            else if (currentDSEncryptionStatus.equals(DeliveryStreamEncryptionStatus.ENABLING_FAILED.toString())
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
                    (int) Duration.ofSeconds(CALLBACK_DELAY_IN_SECONDS).getSeconds(),
                    model);
            }
        }

        try {
            updateDestination(model, describeDeliveryStreamResp);
        }catch (final Exception e) {
            logger.log(String.format("UpdateDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        EncryptionAction encryptionAction = getEncryptionActionToPerform(
            model, describeDeliveryStreamResp);
        try {
            updateEncryptionOnDeliveryStream(model, encryptionAction, logger);
        }catch (final Exception e) {
            logger.log(String.format("updateEncryptionOnDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        try {
            updateTagsOnDeliveryStream(model, logger);
        } catch (final Exception e) {
            logger.log(String
                .format("updateTagsOnDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        // If no encryption action was performed, mark this as success as per existing flow, no need to callback.
        if (encryptionAction == EncryptionAction.DO_NOTHING) {
            logger.log(String
                .format("No Encryption action was performed. Marking the update handler as success."));
            return ProgressEvent.defaultSuccessHandler(model);
        }
        // If the delivery stream encryption was either Started or stopped, it is supposed to have a status.
        val describeResp = firehoseAPIWrapper.describeDeliveryStream(model.getDeliveryStreamName());
        return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                .deliveryStreamStatus(describeResp.deliveryStreamDescription().deliveryStreamStatusAsString())
                .deliveryStreamEncryptionStatus(describeResp.deliveryStreamDescription().deliveryStreamEncryptionConfiguration().statusAsString())
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build(),
            (int) Duration.ofSeconds(CALLBACK_DELAY_IN_SECONDS).getSeconds(),
            model);
    }

    private String getErrorMessageFromEncryptionStatus(String deliveryStreamEncryptionStatus) {
        if (DeliveryStreamEncryptionStatus.ENABLING_FAILED.toString().equals(deliveryStreamEncryptionStatus)) {
            return String.format(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "start");
        }
        return String.format(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "stop");
    }

    private EncryptionAction getEncryptionActionToPerform(ResourceModel model,
        DescribeDeliveryStreamResponse describeResponse) {
        EncryptionAction encryptionAction = EncryptionAction.DO_NOTHING;
        val modelDSEncryptionConfig = model.getDeliveryStreamEncryptionConfigurationInput();
        val existingDSEncryptionConfig = describeResponse.deliveryStreamDescription().deliveryStreamEncryptionConfiguration();
        if (modelDSEncryptionConfig != null) {
            // For CUSTOMER_MANAGED_CMK, if you invoke StartDeliveryStream Encryption again with the same KMS key, firehose returns an error.
            if (existingDSEncryptionConfig != null
                && KeyType.CUSTOMER_MANAGED_CMK.toString().equals(modelDSEncryptionConfig.getKeyType())
                && KeyType.CUSTOMER_MANAGED_CMK.toString().equals(existingDSEncryptionConfig.keyType().toString())
                && existingDSEncryptionConfig.keyARN().equals(modelDSEncryptionConfig
                .getKeyARN())) {
                return encryptionAction;
            }
            encryptionAction = EncryptionAction.START;
        }
        else if (existingDSEncryptionConfig != null && !existingDSEncryptionConfig.statusAsString().equals(DeliveryStreamEncryptionStatus.DISABLED.toString())) {
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
        final ResourceModel model, EncryptionAction encryptionAction, final Logger logger) {
        switch (encryptionAction) {
            case START:
                logger.log(String.format("Starting delivery stream encryption for the delivery stream name %s", model.getDeliveryStreamName()));
                firehoseAPIWrapper.startDeliveryStreamEncryption(model.getDeliveryStreamName(), HandlerUtils.translateDeliveryStreamEncryptionConfigurationInput(model.getDeliveryStreamEncryptionConfigurationInput()));
                break;
            case STOP:
                logger.log(String.format("Stopping delivery stream encryption for the delivery stream name %s", model.getDeliveryStreamName()));
                firehoseAPIWrapper.stopDeliveryStreamEncryption(model.getDeliveryStreamName());
                break;
            default:
                break;
        }
    }

    private void updateDestination(
        ResourceModel model, DescribeDeliveryStreamResponse describeResponse) {
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
            firehoseAPIWrapper.updateDestination(updateDestinationRequest);
    }

    private void updateTagsOnDeliveryStream(ResourceModel model, Logger logger) {
        val existingTags = firehoseAPIWrapper
            .listAllTagsOnDeliveryStream(model.getDeliveryStreamName(), HandlerUtils.LIST_TAGS_PAGE_SIZE);
        logger.log(String.format("Retrieved %d existing tags for the delivery stream name:%s",
            existingTags.size(), model.getDeliveryStreamName()));
        val tagsToAdd = HandlerUtils.translateCFNModelTagsToFirehoseSDKTags(model.getTags());
        val tagKeysToRemove = HandlerUtils.tagKeysInFirstListButNotInSecond(existingTags, tagsToAdd);
        if (tagKeysToRemove != null && !tagKeysToRemove.isEmpty()) {
            firehoseAPIWrapper.untagDeliveryStream(model.getDeliveryStreamName(), tagKeysToRemove);
            logger.log(String
                .format("Removed %d existing tags for the delivery stream name:%s",
                    tagKeysToRemove.size(),
                    model.getDeliveryStreamName()));
        }
        if (tagsToAdd != null && !tagsToAdd.isEmpty()) {
            firehoseAPIWrapper.tagDeliveryStream(model.getDeliveryStreamName(), tagsToAdd);
            logger.log(String
                .format("Added/Replaced %d tags for the delivery stream name:%s", tagsToAdd.size(),
                    model.getDeliveryStreamName()));
        }
    }
}
