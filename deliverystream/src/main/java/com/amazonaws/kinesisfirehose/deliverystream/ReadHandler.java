package com.amazonaws.kinesisfirehose.deliverystream;

import java.util.Optional;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.*;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

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
        val returnModel = describeDeliveryStreamRequest(model);
        if (returnModel.isPresent()) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(returnModel.get())
                    .status(OperationStatus.SUCCESS)
                    .build();
        } else {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .status(OperationStatus.FAILED)
                    .build();
        }
    }

    private Optional<ResourceModel> describeDeliveryStreamRequest(ResourceModel model) {
        val req = DescribeDeliveryStreamRequest.builder()
                .deliveryStreamName(model.getDeliveryStreamName())
                .build();
        try {
            val des =  clientProxy.injectCredentialsAndInvokeV2(req,
                    firehoseClient::describeDeliveryStream)
                    .deliveryStreamDescription();
            model.setArn(des.deliveryStreamARN());
            model.setId(des.deliveryStreamName());
            model.setKinesisStreamSourceConfiguration(HandlerUtils.translateKinesisStreamSourceConfiguration(des.source()));
            model.setDeliveryStreamType(des.deliveryStreamStatusAsString());
            return Optional.of(setDestination(model, des.destinations()));
        } catch (ResourceNotFoundException e) {
            return Optional.empty();
        } catch (FirehoseException e) {
            throw new CfnGeneralServiceException(e.getMessage());
        }
    }

    private ResourceModel setDestination(ResourceModel model, List<DestinationDescription> descriptions) {
        descriptions.stream().forEach(destination -> {
            model.setS3DestinationConfiguration(
                    HandlerUtils.translateS3DestinationConfiguration(destination.s3DestinationDescription()));
            model.setExtendedS3DestinationConfiguration(
                    HandlerUtils.translateExtendedS3DestinationConfiguration(destination.extendedS3DestinationDescription()));
            model.setRedshiftDestinationConfiguration(
                    HandlerUtils.translateRedshiftDestination(destination.redshiftDestinationDescription()));
            model.setElasticsearchDestinationConfiguration(
                    HandlerUtils.translateElasticsearchDestinationConfiguration(destination.elasticsearchDestinationDescription()));
            model.setSplunkDestinationConfiguration(
                    HandlerUtils.translateSplunkDestinationConfiguration(destination.splunkDestinationDescription()));
        });
        return model;
    }

}
