package com.amazonaws.kinesisfirehose.deliverystream;

import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

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

        try {
            logger.log(String.format("Update Handler called with deliveryStream PrimaryId %s", model.getDeliveryStreamName()));
            val describeResponse =  clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                    .deliveryStreamName(model.getDeliveryStreamName())
                    .build(),
                    firehoseClient::describeDeliveryStream);
            return updateDeliveryStream(model, describeResponse, logger);
        } catch (final Exception e) {
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateDeliveryStream(ResourceModel model,
                                                                               DescribeDeliveryStreamResponse describeResponse,
                                                                               final Logger logger) {
        val updateDestinationRequest = UpdateDestinationRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .currentDeliveryStreamVersionId(describeResponse.deliveryStreamDescription().versionId())
                .destinationId(describeResponse.deliveryStreamDescription().destinations().get(0).destinationId())
                .s3DestinationUpdate(HandlerUtils.translateS3DestinationUpdate(model.getS3DestinationConfiguration()))
                .extendedS3DestinationUpdate(HandlerUtils.translateExtendedS3DestinationUpdate(model.getExtendedS3DestinationConfiguration()))
                .redshiftDestinationUpdate(HandlerUtils.translateRedshiftDestinationUpdate(model.getRedshiftDestinationConfiguration()))
                .elasticsearchDestinationUpdate(HandlerUtils.translateElasticsearchDestinationUpdate(model.getElasticsearchDestinationConfiguration()))
                .splunkDestinationUpdate(HandlerUtils.translateSplunkDestinationUpdate(model.getSplunkDestinationConfiguration()))
                .build();

        clientProxy.injectCredentialsAndInvokeV2(updateDestinationRequest, firehoseClient::updateDestination);
        return ProgressEvent.defaultSuccessHandler(model);
    }
}
