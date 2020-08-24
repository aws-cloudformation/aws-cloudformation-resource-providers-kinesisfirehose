package com.amazonaws.kinesisfirehose.deliverystream;

import org.junit.jupiter.api.Disabled;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfiguration;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionStatus;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DestinationDescription;
import software.amazon.awssdk.services.firehose.model.LimitExceededException;
import software.amazon.awssdk.services.firehose.model.ResourceInUseException;
import software.amazon.awssdk.services.firehose.model.ServiceUnavailableException;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationResponse;
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

import static com.amazonaws.kinesisfirehose.deliverystream.DeliveryStreamTestHelper.*;
import static com.amazonaws.kinesisfirehose.deliverystream.UpdateHandler.TIMED_OUT_ENCRYPTION_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static com.amazonaws.kinesisfirehose.deliverystream.UpdateHandler.NUMBER_OF_STATUS_POLL_RETRIES;

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

    // Delivery stream from Disable to Enabling.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnabling() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
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

        final DescribeDeliveryStreamResponse describeResponseSSEEnabling = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLING)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
            .build();
        final StartDeliveryStreamEncryptionResponse startResponse = StartDeliveryStreamEncryptionResponse.builder()
            .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponse).thenReturn(describeResponseSSEEnabling);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenReturn(updateResponse);
        when(proxy.injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class),
            any())).thenReturn(startResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
            .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel().getDeliveryStreamEncryptionConfigurationInput())
            .isEqualToComparingFieldByField(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from DISABLED to ENABLING. Exception on updateDestination.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingWhenUpdateDestinationReturnsException() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenThrow(ResourceInUseException.class);


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ResourceConflict);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from DISABLED to ENABLING. Exception on updateDeliveryStreamEncryption.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingWhenStartDeliveryStreamEncryptionReturnsException() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        UpdateDestinationResponse updateResp = UpdateDestinationResponse.builder().build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenReturn(updateResp);
        when(proxy.injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class),
            any())).thenThrow(LimitExceededException.class);


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceLimitExceeded);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from ENABLING to ENABLED.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnabled() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.ENABLING.toString())
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, callbackContext, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
            .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel().getDeliveryStreamEncryptionConfigurationInput())
            .isEqualToComparingFieldByField(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from ENABLING to ENABLING_FAILED.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingFailed() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnablingFailed = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLING_FAILED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnablingFailed);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.ENABLING.toString())
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, callbackContext, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
            .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel().getDeliveryStreamEncryptionConfigurationInput())
            .isEqualToComparingFieldByField(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from Enabling-Failed to CFN Failure due to timing out because of retry exhaustion.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingFailedWhenCFNTimedOut() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.ENABLING_FAILED.toString())
            .stabilizationRetriesRemaining(0)
            .build();
        ProgressEvent<ResourceModel, CallbackContext> response=null;
        try {
            response = handler.handleRequest(proxy, request, callbackContext, logger);
        }
        catch(Exception e){
            assertThat(e).isInstanceOf(RuntimeException.class).hasMessageContaining(TIMED_OUT_ENCRYPTION_MESSAGE);
        }
        assertThat(response).isNull();
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }
    // Delivery stream from ENABLED to DISABLING.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionDisabling() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        final DescribeDeliveryStreamResponse describeResponseSSEDisabling = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.DISABLING)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        final UpdateDestinationResponse updateResponse = UpdateDestinationResponse.builder()
            .build();
        final StopDeliveryStreamEncryptionResponse stopResponse = StopDeliveryStreamEncryptionResponse.builder()
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled).thenReturn(describeResponseSSEDisabling);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenReturn(updateResponse);
        when(proxy.injectCredentialsAndInvokeV2(any(StopDeliveryStreamEncryptionRequest.class),
            any())).thenReturn(stopResponse);


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
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(2)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from ENABLED to DISABLING. Exception on updateDestination.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionDisablingWhenUpdateDestinationReturnsException() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenThrow(ResourceInUseException.class);


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ResourceConflict);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from ENABLED to DISABLING. Exception on StopDeliveryStreamEncryption.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionDisablingWhenStopDeliveryStreamReturnsException() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.ENABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();
        UpdateDestinationResponse updateResp = UpdateDestinationResponse.builder().build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled);
        when(proxy.injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any())).thenReturn(updateResp);
        when(proxy.injectCredentialsAndInvokeV2(any(StopDeliveryStreamEncryptionRequest.class),
            any())).thenThrow(LimitExceededException.class);


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceLimitExceeded);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from DISABLING to DISABLED.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionDisabled() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEDisabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.DISABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEDisabled);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.DISABLING.toString())
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, callbackContext, logger);
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
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from DISABLING to DISABLING_FAILED.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionDisablingFailed() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEDisablingFailed = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(KMS_KEY_ARN)
                        .keyType(DELIVERY_STREAM_KEY_TYPE)
                        .status(DeliveryStreamEncryptionStatus.DISABLING_FAILED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEDisablingFailed);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.DISABLING.toString())
            .stabilizationRetriesRemaining(NUMBER_OF_STATUS_POLL_RETRIES)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, callbackContext, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
            .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
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
