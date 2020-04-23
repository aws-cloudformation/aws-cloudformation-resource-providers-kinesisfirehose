package com.amazonaws.kinesisfirehose.deliverystream;

import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsRequest;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.val;

public class ListHandler extends BaseHandler<CallbackContext> {
    static final int LIST_RESULT_LIMIT = 50;

    private AmazonWebServicesClientProxy clientProxy;
    private final FirehoseClient firehoseClient = FirehoseClient.create();

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;


        List<ResourceModel> models = new ArrayList<>();
        try {
            val response = listDeliveryStream(request);
            val deliveryStreams = response.deliveryStreamNames();
            models.addAll(deliveryStreams.stream()
                    .map(deliverystream ->
                            ResourceModel.builder().id(deliverystream).build())
                    .collect(Collectors.toList()));
            if (response.deliveryStreamNames().size() == 0 || !response.hasMoreDeliveryStreams()) {
                request.setNextToken(null);
            } else {
                request.setNextToken(deliveryStreams.get(deliveryStreams.size() - 1));
            }

            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(models)
                    .nextToken(request.getNextToken())
                    .status(OperationStatus.SUCCESS)
                    .build();
        } catch (Exception e) {
            return ProgressEvent.defaultFailureHandler(e, ExceptionMapper.mapToHandlerErrorCode(e));
        }
    }

    private ListDeliveryStreamsResponse listDeliveryStream(ResourceHandlerRequest<ResourceModel> request) {
        val req = ListDeliveryStreamsRequest.builder()
                .limit(LIST_RESULT_LIMIT)
                .exclusiveStartDeliveryStreamName(request.getNextToken())
                .build();
        return clientProxy.injectCredentialsAndInvokeV2(req, firehoseClient::listDeliveryStreams);
    }
}
