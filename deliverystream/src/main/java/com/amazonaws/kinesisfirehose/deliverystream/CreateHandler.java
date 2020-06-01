package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.ResourceInUseException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

import java.time.Duration;

import lombok.val;

public class CreateHandler extends BaseHandler<CallbackContext> {
    private static final String STACK_NAME_TAG_KEY = "aws:cloudformation:stack-name";
    private static final String DEFAULT_DELIVERY_STREAM_NAME_PREFIX = "deliverystream";
    private static final int MAX_LENGTH_DELIVERY_STREAM_NAME = 64;
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 130;
    static final String TIMED_OUT_MESSAGE = "Timed out waiting for the delivery stream to become ACTIVE.";

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

        logger.log(String.format("Create Handler called with deliveryStreamName %s", model.getDeliveryStreamName()));
        final CallbackContext currentContext = callbackContext == null
                ? CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build()
                : callbackContext;

        if(callbackContext == null && HandlerUtils.doesDeliveryStreamExistWithName(model,
                clientProxy, firehoseClient)) {
            final Exception e = ResourceInUseException.builder()
                    .message("Firehose already exists with the name: " + model.getDeliveryStreamName())
                    .build();
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        if (StringUtils.isNullOrEmpty(model.getDeliveryStreamName())) {
            model.setDeliveryStreamName(
                    generateName(request)
            );
        }

        // This Lambda will continually be re-invoked with the current state of the instance, finally succeeding when state stabilizes.
        return createDeliveryStreamAndUpdateProgress(model, currentContext, logger);
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDeliveryStreamAndUpdateProgress(ResourceModel model,
                                                                                                CallbackContext callbackContext,
                                                                                                final Logger logger) {
        val deliveryStreamStatus = callbackContext.getDeliveryStreamStatus();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (deliveryStreamStatus == null) {
            try {
                return createDeliveryStream(model);
            } catch (final Exception e) {
                logger.log(String.format("createDeliveryStream failed with exception %s", e.getMessage()));
                return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
            }
        } else {
            val currentDeliveryStreamStatus = getDeliveryStreamStatus(model);
            if (currentDeliveryStreamStatus.equals(DeliveryStreamStatus.ACTIVE.toString())) {
                return ProgressEvent.defaultSuccessHandler(model);
            } else {
                return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                                .deliveryStreamStatus(currentDeliveryStreamStatus)
                                .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                                .build(),
                        (int) Duration.ofSeconds(30).getSeconds(),
                        model);
            }
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> createDeliveryStream(ResourceModel model) {
        val createDeliveryStreamRequest = CreateDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .deliveryStreamType(model.getDeliveryStreamType())
                .s3DestinationConfiguration(HandlerUtils.translateS3DestinationConfiguration(model.getS3DestinationConfiguration()))
                .extendedS3DestinationConfiguration(HandlerUtils.translateExtendedS3DestinationConfiguration(model.getExtendedS3DestinationConfiguration()))
                .redshiftDestinationConfiguration(HandlerUtils.translateRedshiftDestinationConfiguration(model.getRedshiftDestinationConfiguration()))
                .elasticsearchDestinationConfiguration(HandlerUtils.translateElasticsearchDestinationConfiguration(model.getElasticsearchDestinationConfiguration()))
                .kinesisStreamSourceConfiguration(HandlerUtils.translateKinesisStreamSourceConfiguration(model.getKinesisStreamSourceConfiguration()))
                .splunkDestinationConfiguration(HandlerUtils.translateSplunkDestinationConfiguration(model.getSplunkDestinationConfiguration()))
                .build();

        //Firehose API returns an ARN on create, but does not accept ARN for any of its operations that act on a DeliveryStream
        //This is why DeliveryStream name is the physical resource ID and not the ARN
        val response = clientProxy.injectCredentialsAndInvokeV2(createDeliveryStreamRequest, firehoseClient::createDeliveryStream);
        model.setArn(response.deliveryStreamARN());
        return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                .deliveryStreamStatus(getDeliveryStreamStatus(model))
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build(),
                (int)Duration.ofSeconds(30).getSeconds(),
                model);
    }

    private String getDeliveryStreamStatus(ResourceModel model) {
        val response = clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .build(),
                firehoseClient::describeDeliveryStream);
        return response.deliveryStreamDescription().deliveryStreamStatusAsString();
    }

    @VisibleForTesting
    protected static String generateName(ResourceHandlerRequest<ResourceModel> request) {
        StringBuffer identifierPrefix = new StringBuffer();
        // the prefix will be <stack-name>-<resource type>
        identifierPrefix.append((request.getSystemTags() != null && request.getSystemTags().containsKey(STACK_NAME_TAG_KEY)) ?
                request.getSystemTags().get(STACK_NAME_TAG_KEY) + "-" :
                "");
        identifierPrefix.append(request.getLogicalResourceIdentifier() == null ?
                DEFAULT_DELIVERY_STREAM_NAME_PREFIX :
                request.getLogicalResourceIdentifier());
        // This utility function will add the auto-generated ID after the prefix.
        String name = IdentifierUtils.generateResourceIdentifier(
                identifierPrefix.toString(),
                request.getClientRequestToken(),
                MAX_LENGTH_DELIVERY_STREAM_NAME);

        return name;
    }
}
