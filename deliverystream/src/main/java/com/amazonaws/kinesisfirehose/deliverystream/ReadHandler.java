package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.kinesisfirehose.deliverystream.HandlerUtils.HandlerType;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {

    private FirehoseAPIWrapper firehoseAPIWrapper;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {
        firehoseAPIWrapper = FirehoseAPIWrapper.builder().firehoseClient(FirehoseClient.create()).clientProxy(proxy).build();
        final ResourceModel model = request.getDesiredResourceState();
        logger.log(String.format("Read Handler called with id %s.", model.getDeliveryStreamName()));
        try {
            val deliveryStreamDescription = firehoseAPIWrapper.describeDeliveryStream(model.getDeliveryStreamName()).deliveryStreamDescription();
            val tags = firehoseAPIWrapper
                .listAllTagsOnDeliveryStream(model.getDeliveryStreamName(), HandlerUtils.LIST_TAGS_RESULT_LIMIT);
            HandlerUtils.hydrateDeliveryStreamResource(model, logger, deliveryStreamDescription, tags);
            logger.log(String.format("Hydrated deliveryStream model with %d retrieved tags on the delivery stream name %s", tags.size(), model.getDeliveryStreamName()));
            return ProgressEvent.defaultSuccessHandler(model);
        } catch (Exception e) {
            logger.log(String.format("Got exception for %s, error message %s",
                model.getDeliveryStreamName(),
                e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.READ));
        }
    }
}
