package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.util.StringUtils;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.ResourceInUseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

import java.time.Duration;

public class CreateHandler extends BaseHandler<CallbackContext> {
    static final int NUMBER_OF_STATUS_POLL_RETRIES = 40;
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

        final CallbackContext currentContext = callbackContext == null
                ? CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .build()
                : callbackContext;

        if(callbackContext == null && doesDeliveryStreamExistWithName(model)) {
            final Exception e = ResourceInUseException.builder()
                    .message("Firehose already exists with the name: " + model.getDeliveryStreamName())
                    .build();
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }

        if (StringUtils.isNullOrEmpty(model.getDeliveryStreamName())) {
            model.setDeliveryStreamName(
                    IdentifierUtils.generateResourceIdentifier(
                            request.getLogicalResourceIdentifier(),
                            request.getClientRequestToken()
                    )
            );
        }

        // This Lambda will continually be re-invoked with the current state of the instance, finally succeeding when state stabilizes.
        return createDeliveryStreamAndUpdateProgress(model, currentContext);
    }


    private ProgressEvent<ResourceModel, CallbackContext> createDeliveryStreamAndUpdateProgress(ResourceModel model, CallbackContext callbackContext) {
        val deliveryStreamStatus = callbackContext.getDeliveryStreamStatus();

        if (callbackContext.getStabilizationRetriesRemaining() == 0) {
            throw new RuntimeException(TIMED_OUT_MESSAGE);
        }

        if (deliveryStreamStatus == null) {
            try {
                return createDeliveryStream(model);
            } catch (final Exception e) {
                return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
            }
        } else if (deliveryStreamStatus.equals(DeliveryStreamStatus.ACTIVE.toString())) {
            return ProgressEvent.defaultSuccessHandler(model);
        } else {
            return ProgressEvent.defaultInProgressHandler(CallbackContext.builder()
                    .deliveryStreamStatus(getDeliveryStreamStatus(model))
                    .stabilizationRetriesRemaining(callbackContext.getStabilizationRetriesRemaining() - 1)
                    .build(),
                    (int)Duration.ofSeconds(30).getSeconds(),
                    model);
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
        model.setId(model.getDeliveryStreamName());
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

    private boolean doesDeliveryStreamExistWithName(ResourceModel model) {
        if(StringUtils.isNullOrEmpty(model.getDeliveryStreamName())) {
            return false;
        }

        try {
            clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                    .deliveryStreamName(model.getDeliveryStreamName())
                    .build(),
                    firehoseClient::describeDeliveryStream);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }
}
