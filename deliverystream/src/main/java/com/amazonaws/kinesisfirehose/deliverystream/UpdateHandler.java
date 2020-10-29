package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.kinesisfirehose.deliverystream.HandlerUtils.HandlerType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfiguration;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
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
    static final String ACCESS_DENIED_FOR_SPECIFIED_API_FORMAT= "Got Access denied exception from backend service for %s API for delivery stream name: %s."
        + " Going to do a soft fail(Will not mark the handler as failure).";

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final ResourceModel previousModel = request.getPreviousResourceState();
        firehoseAPIWrapper = FirehoseAPIWrapper.builder().firehoseClient(FirehoseClient.create()).clientProxy(proxy).build();
        logger.log(String.format("Update Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));
        val currentContext = callbackContext != null
            ? callbackContext : CallbackContext.builder()
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();

        List<Tag> previousResourceAndStackTags = new ArrayList<Tag>();
        if (request.getPreviousResourceTags() != null && !request.getPreviousResourceTags().isEmpty()) {
            request.getPreviousResourceTags().forEach((k,v) -> previousResourceAndStackTags.add(new Tag(k, v)));
            logger.log(String.format("Received %d Previous Resource tags on update for delivery stream name %s", previousResourceAndStackTags.size(), model.getDeliveryStreamName()));
        }

        List<Tag> currentResourceAndStackTags = new ArrayList<Tag>();
        if (request.getDesiredResourceTags() != null && !request.getDesiredResourceTags().isEmpty()) {
            request.getDesiredResourceTags().forEach((k,v) -> currentResourceAndStackTags.add(new Tag(k, v)));
            logger.log(String.format("Received %d current Resource tags on update for delivery stream name %s", previousResourceAndStackTags.size(), model.getDeliveryStreamName()));
        }
        return updateDeliveryStreamAndUpdateProgress(model, previousModel, currentContext, logger, previousResourceAndStackTags, currentResourceAndStackTags);
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDeliveryStreamAndUpdateProgress(
        ResourceModel model, ResourceModel previousModel,  CallbackContext callbackContext, final Logger logger, List<Tag> previousResourceAndStackTags, List<Tag> currentResourceAndStackTags) {
        val deliveryStreamEncryptionStatus = callbackContext.getDeliveryStreamEncryptionStatus();
        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }
        DescribeDeliveryStreamResponse describeDeliveryStreamResp;
        try {
            describeDeliveryStreamResp = firehoseAPIWrapper.describeDeliveryStream(model.getDeliveryStreamName());
        } catch (ResourceNotFoundException e) {
            logger.log(String.format("DescribeDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.UPDATE));
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
                return ProgressEvent.defaultFailureHandler(exp, ExceptionMapper.mapToHandlerErrorCode(exp, HandlerType.UPDATE));
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
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.UPDATE));
        }

        EncryptionAction encryptionAction = getEncryptionActionToPerform(
            model, describeDeliveryStreamResp);
        try {
            updateEncryptionOnDeliveryStream(model, encryptionAction, logger);
        }catch (final Exception e) {
            logger.log(String.format("updateEncryptionOnDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.UPDATE));
        }

        try {
            updateTagsOnDeliveryStream(model, previousModel, logger, previousResourceAndStackTags, currentResourceAndStackTags);
        } catch (final Exception e) {
            logger.log(String
                .format("updateTagsOnDeliveryStream failed with exception %s", e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e,HandlerType.UPDATE));
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
            if (areEncryptionParametersUnchanged(modelDSEncryptionConfig, existingDSEncryptionConfig)) {
                return encryptionAction;
            }
            encryptionAction = EncryptionAction.START;
        }
        else if (existingDSEncryptionConfig != null && !DeliveryStreamEncryptionStatus.DISABLED.toString().equals(existingDSEncryptionConfig.statusAsString())) {
            encryptionAction = EncryptionAction.STOP;
        }
        return encryptionAction;
    }

    // Basically tries to make sure that we don't try to start encryption in cases of AWS_OWNED_CMK -> AWS_OWNED_CMK or CUSTOMER_MANAGED_CMK(Key1) -> CUSTOMER_MANAGED_CMK(Key1)
    // as the firehose backend fails in those cases.
    private boolean areEncryptionParametersUnchanged(DeliveryStreamEncryptionConfigurationInput modelDSEncryptionConfig, DeliveryStreamEncryptionConfiguration existingDSEncryptionConfig){
        return existingDSEncryptionConfig != null
            && ((KeyType.CUSTOMER_MANAGED_CMK.toString().equals(modelDSEncryptionConfig.getKeyType())
                && (existingDSEncryptionConfig.keyType() != null && KeyType.CUSTOMER_MANAGED_CMK
                .toString()
                .equals(existingDSEncryptionConfig.keyType().toString()))
                && modelDSEncryptionConfig
                .getKeyARN().equals(existingDSEncryptionConfig.keyARN()))
                ||
                (KeyType.AWS_OWNED_CMK.toString().equals(modelDSEncryptionConfig.getKeyType())
                    && (existingDSEncryptionConfig.keyType() != null && KeyType.AWS_OWNED_CMK
                    .toString().equals(existingDSEncryptionConfig.keyType().toString()))));
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

    private void updateTagsOnDeliveryStream(ResourceModel model, ResourceModel previousModel,
         Logger logger, List<Tag> previousResourceAndStackTags, List<Tag> currentResourceAndStackTags) {
        val tagKeysToRemove = HandlerUtils.tagsInFirstListButNotInSecond(previousResourceAndStackTags, currentResourceAndStackTags);
        if (tagKeysToRemove != null && !tagKeysToRemove.isEmpty()) {
            boolean wasExceptionThrown = false;
            try {
                firehoseAPIWrapper.untagDeliveryStream(model.getDeliveryStreamName(), tagKeysToRemove);
            } catch(Exception e){
                wasExceptionThrown = true;
                // If previous model didn't had any resource tags, and we tried to remove tags only because of stack level tags, this might be unexpected from
                // a customer standpoint and might look like a breaking API change. We need to do a soft fail.
                if (customerDidNotSpecifiedModelTags(previousModel, model) && (e instanceof FirehoseException
                    && ((FirehoseException) e).awsErrorDetails() != null
                    && (HandlerUtils.ACCESS_DENIED_ERROR_CODE
                    .equals(((FirehoseException) e).awsErrorDetails().errorCode())))) {
                    logger.log(String.format(ACCESS_DENIED_FOR_SPECIFIED_API_FORMAT, "UntagDeliveryStream",  model.getDeliveryStreamName()));
                } else {
                    // Surface the error to the customer if they explicitly wanted to use tags, or if we ran into a different error while talking to the backend API.
                    throw e;
                }
            }
            if (!wasExceptionThrown) {
                logger.log(String.format("Removed %d existing tags for the delivery stream name:%s",tagKeysToRemove.size(),
                        model.getDeliveryStreamName()));
            }
        }

        if (currentResourceAndStackTags != null && !currentResourceAndStackTags.isEmpty()) {
            boolean wasExceptionThrown = false;
            try {
                firehoseAPIWrapper.tagDeliveryStream(model.getDeliveryStreamName(), HandlerUtils.translateCFNModelTagsToFirehoseSDKTags(currentResourceAndStackTags));
            } catch(Exception e){
                wasExceptionThrown = true;
                // If current model didn't had any resource tags, and we tried to add tags during update only because of stack level tags, this might be unexpected from
                // a customer standpoint and might look like a breaking API change if the customer didn't had permissions. We need to do a soft fail.
                if (customerDidNotSpecifiedModelTags(previousModel, model) && (e instanceof FirehoseException
                    && ((FirehoseException)e).awsErrorDetails().errorCode().equals(HandlerUtils.ACCESS_DENIED_ERROR_CODE))){
                    logger.log(String.format(ACCESS_DENIED_FOR_SPECIFIED_API_FORMAT, "TagDeliveryStream", model.getDeliveryStreamName()));
                } else {
                    // Surface the error to the customer if they explicitly wanted to use tags, or if we ran into a different error while talking to the backend API.
                    throw e;
                }
            }
            if (!wasExceptionThrown) {
                logger.log(String.format("Added/Replaced %d tags for the delivery stream name:%s", currentResourceAndStackTags.size(),
                        model.getDeliveryStreamName()));
            }
        }
    }

    private boolean customerDidNotSpecifiedModelTags(ResourceModel previousModel, ResourceModel model) {
        return (previousModel.getTags() == null || previousModel.getTags().isEmpty()) && (model.getTags() == null || model.getTags().isEmpty());
    }

}
