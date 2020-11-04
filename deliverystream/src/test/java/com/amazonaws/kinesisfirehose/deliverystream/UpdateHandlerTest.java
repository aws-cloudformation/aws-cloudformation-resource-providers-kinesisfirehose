package com.amazonaws.kinesisfirehose.deliverystream;

import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DestinationDescription;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.amazonaws.kinesisfirehose.deliverystream.DeliveryStreamTestHelper.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest {
    private UpdateHandler handler;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler();
    }

    @Test
    public void testUpdateDeliverySteamWithS3ExtendedConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .versionId("version-0001")
                        .destinations(DestinationDescription.builder()
                                .destinationId("destination-0001")
                                .build())
                        .build())
                .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);
        doReturn(updateResponse).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
                .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamWithRedshiftConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .redshiftDestinationConfiguration(REDSHIFT_DESTINATION_CONFIGURATION)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .versionId("version-0001")
                        .destinations(DestinationDescription.builder()
                                .destinationId("destination-0001")
                                .build())
                        .build())
                .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);
        doReturn(updateResponse).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getRedshiftDestinationConfiguration())
                .isEqualToComparingFieldByField(REDSHIFT_DESTINATION_CONFIGURATION);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamWithElasticSearchConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .elasticsearchDestinationConfiguration(ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .versionId("version-0001")
                        .destinations(DestinationDescription.builder()
                                .destinationId("destination-0001")
                                .build())
                        .build())
                .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);
        doReturn(updateResponse).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getElasticsearchDestinationConfiguration())
                .isEqualToComparingFieldByField(ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamWithSplunkConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .splunkDestinationConfiguration(SPLUNK_CONFIGURATION_FULL)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .versionId("version-0001")
                        .destinations(DestinationDescription.builder()
                                .destinationId("destination-0001")
                                .build())
                        .build())
                .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);
        doReturn(updateResponse).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getSplunkDestinationConfiguration())
                .isEqualToComparingFieldByField(SPLUNK_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamWithHttpEndpointConfiguration() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .httpEndpointDestinationConfiguration(HTTP_ENDPOINT_DESTINATION_CONFIGURATION)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .versionId("version-0001")
                        .destinations(DestinationDescription.builder()
                                .destinationId("destination-0001")
                                .build())
                        .build())
                .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
                any())).thenReturn(describeResponse);
        doReturn(updateResponse).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
                any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
                = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getHttpEndpointDestinationConfiguration())
                .isEqualToComparingFieldByField(HTTP_ENDPOINT_DESTINATION_CONFIGURATION);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
    }
}
