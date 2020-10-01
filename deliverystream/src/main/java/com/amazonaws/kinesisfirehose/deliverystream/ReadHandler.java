package com.amazonaws.kinesisfirehose.deliverystream;

import com.amazonaws.kinesisfirehose.deliverystream.HandlerUtils.HandlerType;
import java.util.List;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DestinationDescription;
import software.amazon.awssdk.services.firehose.model.Tag;
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
            hydrateDeliveryStreamResource(model, deliveryStreamDescription, tags);
            logger.log(String.format("Hydrated deliveryStream model with %d retrieved tags on the delivery stream name %s", tags.size(), model.getDeliveryStreamName()));
            return ProgressEvent.defaultSuccessHandler(model);
        } catch (Exception e) {
            logger.log(String.format("Got exception for %s, error message %s",
                model.getDeliveryStreamName(),
                e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e, HandlerType.READ));
        }
    }

    private void hydrateDeliveryStreamResource(ResourceModel model, DeliveryStreamDescription deliveryStreamDescription, List<Tag> tags) {
        model.setArn(deliveryStreamDescription.deliveryStreamARN());
        model.setKinesisStreamSourceConfiguration(HandlerUtils.translateKinesisStreamSourceConfigurationToCfnModel(deliveryStreamDescription.source()));
        model.setDeliveryStreamType(deliveryStreamDescription.deliveryStreamTypeAsString());
        model.setDeliveryStreamEncryptionConfigurationInput(HandlerUtils.translateDeliveryStreamEncryptionConfigurationInputToCfnModel(deliveryStreamDescription.deliveryStreamEncryptionConfiguration()));
        setDestinationDescription(model, deliveryStreamDescription.destinations());
        model.setTags(HandlerUtils.translateFirehoseSDKTagsToCfnModelTags(tags.isEmpty() ? null : tags));
    }

    private void setDestinationDescription(final ResourceModel model, final List<DestinationDescription> descriptions) {
        descriptions.stream().forEach(destination -> {
            // If the delivery stream is created with extendedS3Destinations, when you query firehose to populate the state of the model, firehose populates both the
            // s3destination and extendedS3Destination. We need our model to truly reflect the state of the structure with which it was created which was only with extendedS3Destination.
            if (destination.extendedS3DestinationDescription() == null) {
                model.setS3DestinationConfiguration(
                    HandlerUtils.translateS3DestinationConfigurationToCfnModel(destination.s3DestinationDescription()));
            }
            model.setExtendedS3DestinationConfiguration(
                HandlerUtils.translateExtendedS3DestinationConfigurationToCfnModel(destination.extendedS3DestinationDescription()));
            model.setRedshiftDestinationConfiguration(
                HandlerUtils.translateRedshiftDestinationToCfnModel(destination.redshiftDestinationDescription()));
            model.setElasticsearchDestinationConfiguration(
                HandlerUtils.translateElasticsearchDestinationConfigurationToCfnModel(destination.elasticsearchDestinationDescription()));
            model.setSplunkDestinationConfiguration(
                HandlerUtils.translateSplunkDestinationConfigurationToCfnModel(destination.splunkDestinationDescription()));
            model.setHttpEndpointDestinationConfiguration(
                HandlerUtils.translateHttpEndpointDestinationConfigurationToCfnModel(destination.httpEndpointDestinationDescription()));
        });
    }
}
