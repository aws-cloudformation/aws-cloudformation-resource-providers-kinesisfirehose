package com.amazonaws.kinesisfirehose.deliverystream;

import software.amazon.awssdk.services.firehose.FirehoseClient;

import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DestinationDescription;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

import lombok.val;

public class ReadHandler extends BaseHandler<CallbackContext> {

    private AmazonWebServicesClientProxy clientProxy;
    private final FirehoseClient firehoseClient = FirehoseClient.create();
    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        final ResourceModel model = request.getDesiredResourceState();
        logger.log(String.format("Read Handler called with id %s.", model.getDeliveryStreamName()));

        try {
            val returnModel = describeDeliveryStreamRequest(model);
            return ProgressEvent.defaultSuccessHandler(returnModel);
        } catch (Exception e) {
            logger.log(String.format("Got exception for %s, error message %s",
                    model.getDeliveryStreamName(),
                    e.getMessage()));
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }
    }

    private ResourceModel describeDeliveryStreamRequest(ResourceModel model) {
        val req = DescribeDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .build();
        val des =  clientProxy.injectCredentialsAndInvokeV2(req,
                firehoseClient::describeDeliveryStream)
                .deliveryStreamDescription();
        model.setArn(des.deliveryStreamARN());
        model.setKinesisStreamSourceConfiguration(HandlerUtils.translateKinesisStreamSourceConfigurationToCfnModel(des.source()));
        model.setDeliveryStreamType(des.deliveryStreamStatusAsString());
        return setDestinationDescription(model, des.destinations());
    }

    private ResourceModel setDestinationDescription(ResourceModel model, List<DestinationDescription> descriptions) {
        descriptions.stream().forEach(destination -> {
            model.setS3DestinationConfiguration(
                    HandlerUtils.translateS3DestinationConfigurationToCfnModel(destination.s3DestinationDescription()));
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
        return model;
    }
}
