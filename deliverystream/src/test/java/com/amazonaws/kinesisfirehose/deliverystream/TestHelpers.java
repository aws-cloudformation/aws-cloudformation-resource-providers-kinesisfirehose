package com.amazonaws.kinesisfirehose.deliverystream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.ListTagsForDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.ListTagsForDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;

public class TestHelpers {

    public static void stubListTagsForDeliveryStreamWithProvidedOrEmptyResponse(AmazonWebServicesClientProxy proxy, ListTagsForDeliveryStreamResponse response) {
        if (response == null) {
            response =  ListTagsForDeliveryStreamResponse
                .builder()
                .build();
        }
        doReturn(response).when(proxy).injectCredentialsAndInvokeV2(any(
            ListTagsForDeliveryStreamRequest.class),
            any());
    }

    public static void stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(AmazonWebServicesClientProxy proxy, DescribeDeliveryStreamResponse response) {
        if (response == null) {
            response =  DescribeDeliveryStreamResponse
                .builder()
                .build();
        }
        doReturn(response).when(proxy).injectCredentialsAndInvokeV2(any(
            DescribeDeliveryStreamRequest.class),
            any());
    }

    public static void stubUpdateDestinationWithProvidedOrEmptyResponse(AmazonWebServicesClientProxy proxy, UpdateDestinationResponse response) {
        if (response == null) {
            response =  UpdateDestinationResponse
                .builder()
                .build();
        }
        doReturn(response).when(proxy).injectCredentialsAndInvokeV2(any(
            UpdateDestinationRequest.class),
            any());
    }

    public static void stubStopDeliveryStreamEncryptionWithProvidedOrEmptyResponse(AmazonWebServicesClientProxy proxy, StopDeliveryStreamEncryptionResponse response) {
        if (response == null) {
            response =  StopDeliveryStreamEncryptionResponse
                .builder()
                .build();
        }
        doReturn(response).when(proxy).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class),
            any());
    }

    public static void stubStartDeliveryStreamEncryptionWithProvidedOrEmptyResponse(AmazonWebServicesClientProxy proxy, StartDeliveryStreamEncryptionResponse response) {
        if (response == null) {
            response =  StartDeliveryStreamEncryptionResponse
                .builder()
                .build();
        }
        doReturn(response).when(proxy).injectCredentialsAndInvokeV2(any(
            StartDeliveryStreamEncryptionRequest.class),
            any());
    }

}
