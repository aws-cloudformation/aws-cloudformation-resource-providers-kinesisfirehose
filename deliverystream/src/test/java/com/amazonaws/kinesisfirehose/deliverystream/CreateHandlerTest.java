package com.amazonaws.kinesisfirehose.deliverystream;

import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.amazonaws.kinesisfirehose.deliverystream.CreateHandler.NUMBER_OF_STATUS_POLL_RETRIES;
import static com.amazonaws.kinesisfirehose.deliverystream.CreateHandler.TIMED_OUT_MESSAGE;
import static com.amazonaws.kinesisfirehose.deliverystream.DeliveryStreamTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest {
    private CreateHandler handler;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
    }

    @Test
    public void testCreateDeliverySteamWithS3ExtendedConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();
        final CreateDeliveryStreamResponse createResponse = CreateDeliveryStreamResponse.builder()
                .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenThrow(ResourceNotFoundException.builder().build())
                       .thenReturn(describeResponse);
        doReturn(createResponse).when(proxy).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getArn()).isEqualTo(DELIVERY_STREAM_NAME_ARN);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
                .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliveryStreamWithRedshiftConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .redshiftDestinationConfiguration(REDSHIFT_DESTINATION_CONFIGURATION)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();
        final CreateDeliveryStreamResponse createResponse = CreateDeliveryStreamResponse.builder()
                .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(describeResponse);
        doReturn(createResponse).when(proxy).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getArn()).isEqualTo(DELIVERY_STREAM_NAME_ARN);
        assertThat(response.getResourceModel().getRedshiftDestinationConfiguration())
                .isEqualToComparingFieldByField(REDSHIFT_DESTINATION_CONFIGURATION);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliveryStreamWithElasticSearchConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .elasticsearchDestinationConfiguration(ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();
        final CreateDeliveryStreamResponse createResponse = CreateDeliveryStreamResponse.builder()
                .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(describeResponse);
        doReturn(createResponse).when(proxy).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getArn()).isEqualTo(DELIVERY_STREAM_NAME_ARN);
        assertThat(response.getResourceModel().getElasticsearchDestinationConfiguration())
                .isEqualToComparingFieldByField(ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliveryStreamWithSplunkConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .splunkDestinationConfiguration(SPLUNK_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();
        final CreateDeliveryStreamResponse createResponse = CreateDeliveryStreamResponse.builder()
                .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(describeResponse);
        doReturn(createResponse).when(proxy).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getArn()).isEqualTo(DELIVERY_STREAM_NAME_ARN);
        assertThat(response.getResourceModel().getSplunkDestinationConfiguration())
                .isEqualToComparingFieldByField(SPLUNK_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliveryStreamWithSASConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .kinesisStreamSourceConfiguration(KINESIS_STREAM_SOURCE_CONFIGURATION)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();
        final CreateDeliveryStreamResponse createResponse = CreateDeliveryStreamResponse.builder()
                .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenThrow(ResourceNotFoundException.builder().build())
                .thenReturn(describeResponse);
        doReturn(createResponse).when(proxy).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getArn()).isEqualTo(DELIVERY_STREAM_NAME_ARN);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
                .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliverySteamInProgress() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        final CallbackContext desiredOutputContext = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES-1)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isEqualToComparingFieldByField(desiredOutputContext);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
    }

    @Test
    public void testCreateDeliverySteamComplete() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES-1)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, context, logger);

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }


    @Test
    public void testCreateDeliverySteamStabilizationTimeout() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final CallbackContext context = CallbackContext.builder()
                .stabilizationRetriesRemaining(0)
                .deliveryStreamStatus(DeliveryStreamStatus.CREATING.toString())
                .build();

        try {
            handler.handleRequest(proxy, request, context, logger);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo(TIMED_OUT_MESSAGE);
        }
    }

    @Test
    public void testCreateDeliverySteamAlreadyExists() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamType(DELIVERY_STREAM_TYPE)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.CREATING)
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ResourceConflict);
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(CreateDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
    }
}
