package com.amazonaws.kinesisfirehose.deliverystream;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfiguration;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionStatus;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamStatus;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DestinationDescription;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.KeyType;
import software.amazon.awssdk.services.firehose.model.LimitExceededException;
import software.amazon.awssdk.services.firehose.model.ResourceInUseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.TagDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.UntagDeliveryStreamRequest;
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
import static com.amazonaws.kinesisfirehose.deliverystream.UpdateHandler.ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT;
import static com.amazonaws.kinesisfirehose.deliverystream.UpdateHandler.TIMED_OUT_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
    public void testUpdateDeliverySteamWithS3ExtendedConfigurationAndUpdateTags() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
                .build();

        final ResourceModel previousModel = ResourceModel.builder()
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
                .previousResourceState(previousModel)
                .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
                .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
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
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamUpdateTagsSoftFailOnUnTagFailureAccessDenied() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final ResourceModel previousModel = ResourceModel.builder()
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
        doThrow(FirehoseException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(HandlerUtils.ACCESS_DENIED_ERROR_CODE).build())
            .build()).when(proxy)
            .injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(previousModel)
            .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
            .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
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
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamUpdateTagsSoftFailOnTagFailureAccessDenied() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final ResourceModel previousModel = ResourceModel.builder()
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
        TestHelpers.stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(proxy, describeResponse);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, updateResponse);
        TestHelpers.stubUntagDeliveryStreamWithProvidedOrEmptyResponse(proxy, null);
        doThrow(FirehoseException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(HandlerUtils.ACCESS_DENIED_ERROR_CODE).build())
            .build()).when(proxy)
            .injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(previousModel)
            .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
            .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
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
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamUpdateTagsHardFailOnTagFailure() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        final ResourceModel previousModel = ResourceModel.builder()
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
        TestHelpers.stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(proxy, describeResponse);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, updateResponse);
        TestHelpers.stubUntagDeliveryStreamWithProvidedOrEmptyResponse(proxy, null);
        doThrow(FirehoseException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalFailure").build())
            .build()).when(proxy)
            .injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(previousModel)
            .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
            .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
    }
    // Delivery stream from Disable to Enabling with CUSTOMER_MANAGED_CMK
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnabling() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
            .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder().status(DeliveryStreamEncryptionStatus.DISABLED).build())
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
                        .keyType(KeyType.CUSTOMER_MANAGED_CMK)
                        .status(DeliveryStreamEncryptionStatus.ENABLING)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponse).thenReturn(describeResponseSSEEnabling);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);
        TestHelpers.stubStartDeliveryStreamEncryptionWithProvidedOrEmptyResponse(proxy, null);

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
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK);
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

    // Delivery stream from Disable to Enabling with AWS_OWNED_CMK
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingAWSOwnedCMKCheckNPE() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_AWS_OWNED_CMK)
            .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder().status(DeliveryStreamEncryptionStatus.DISABLED).build())
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
                        .keyType(KeyType.AWS_OWNED_CMK)
                        .status(DeliveryStreamEncryptionStatus.ENABLING)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponse).thenReturn(describeResponseSSEEnabling);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);
        TestHelpers.stubStartDeliveryStreamEncryptionWithProvidedOrEmptyResponse(proxy, null);

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
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_AWS_OWNED_CMK);
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
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
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
        doThrow(ResourceInUseException.class).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any());

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
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
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
        doReturn(updateResp).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any());
        doThrow(LimitExceededException.class).when(proxy).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class),
            any());

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

    // Delivery stream from ENABLING to ENABLING with Ongoing stabalization
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingWithOngoingStabalization() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
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
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getCallbackContext().getStabilizationRetriesRemaining()).isEqualTo(NUMBER_OF_STATUS_POLL_RETRIES-1);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from ENABLING to ENABLED.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnabled() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
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
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK);
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

    // Don't start encryption for Delivery stream with the same Customer Managed CMK.
    @Test
    public void testUpdateDeliverySteamWithSSESameCustomerManagedCMK() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyARN(DELIVERY_STREAM_KEY_ARN)
                        .keyType(KeyType.CUSTOMER_MANAGED_CMK)
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

        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getResourceModel().getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(response.getResourceModel().getExtendedS3DestinationConfiguration())
            .isEqualToComparingFieldByField(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL);
        assertThat(response.getResourceModel().getDeliveryStreamEncryptionConfigurationInput())
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Don't start encryption for Delivery stream already enabled with the AWS_OWNED_CMK.
    @Test
    public void testUpdateDeliverySteamWithSSESameAWSOwnedCMK() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_AWS_OWNED_CMK)
            .build();

        final DescribeDeliveryStreamResponse describeResponseSSEEnabled = DescribeDeliveryStreamResponse.builder()
            .deliveryStreamDescription(DeliveryStreamDescription.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .versionId("version-0001")
                .deliveryStreamEncryptionConfiguration(
                    DeliveryStreamEncryptionConfiguration.builder()
                        .keyType(KeyType.AWS_OWNED_CMK)
                        .status(DeliveryStreamEncryptionStatus.ENABLED)
                        .build())
                .destinations(DestinationDescription.builder()
                    .destinationId("destination-0001")
                    .build())
                .build())
            .build();

        TestHelpers.stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(proxy, describeResponseSSEEnabled);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);

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
            .isEqualToComparingFieldByField(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_AWS_OWNED_CMK);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
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
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "start");
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    // Delivery stream from Enabling to CFN Failure due to timing out because of retry exhaustion.
    @Test
    public void testUpdateDeliverySteamWithSSEEncryptionEnablingFailedWhenCFNTimedOut() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .deliveryStreamEncryptionConfigurationInput(
                DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT_CUSTOMER_MANAGED_CMK)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        CallbackContext callbackContext = CallbackContext.builder()
            .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE.toString())
            .deliveryStreamEncryptionStatus(DeliveryStreamEncryptionStatus.ENABLING.toString())
            .stabilizationRetriesRemaining(0)
            .build();
        ProgressEvent<ResourceModel, CallbackContext> response=null;
        try {
            response = handler.handleRequest(proxy, request, callbackContext, logger);
        }
        catch(Exception e) {
            assertThat(e).isInstanceOf(RuntimeException.class).hasMessageContaining(
                TIMED_OUT_MESSAGE);
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

        final ResourceModel previousModel = ResourceModel.builder()
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
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenReturn(describeResponseSSEEnabled).thenReturn(describeResponseSSEDisabling);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);
        TestHelpers.stubStopDeliveryStreamEncryptionWithProvidedOrEmptyResponse(proxy, null);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(previousModel)
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
        doThrow(ResourceInUseException.class).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any());

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
        doReturn(updateResp).when(proxy).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class),
            any());
        doThrow(LimitExceededException.class).when(proxy).injectCredentialsAndInvokeV2(any(StopDeliveryStreamEncryptionRequest.class),
            any());


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

    @Test
    public void testUpdateDeliveryStreamDescribeDSThrowsResourceNotFoundException(){
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenThrow(ResourceNotFoundException.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }

    @Test
    public void testUpdateDeliveryStreamDescribeDSThrowsFirehoseException(){
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class),
            any())).thenThrow(FirehoseException.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);

        assertThat(response).isNotNull();
        assertThat(response.getResourceModel()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getResourceModels()).isNull();
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isEqualTo(String.format(ERROR_DELIVERY_STREAM_ENCRYPTION_FORMAT, "stop"));
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(StartDeliveryStreamEncryptionRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(
            StopDeliveryStreamEncryptionRequest.class), any());
    }


    @Test
    public void testUpdateDeliverySteamWithRedshiftConfigurationAddTags() {
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
                .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
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
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
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
    public void testUpdateDeliverySteamWithHttpEndpointConfigurationAndRemoveAllTags() {
        final ResourceModel model = ResourceModel.builder()
                .deliveryStreamName(DELIVERY_STREAM_NAME)
                .httpEndpointDestinationConfiguration(HTTP_ENDPOINT_DESTINATION_CONFIGURATION)
                .build();
        final ResourceModel previousModel = ResourceModel.builder()
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
        TestHelpers.stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(proxy, describeResponse);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, null);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .previousResourceState(previousModel)
                .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
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
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
    }

    @Test
    public void testUpdateDeliverySteamUpdateTagsFailsWithException() {
        final ResourceModel model = ResourceModel.builder()
            .deliveryStreamName(DELIVERY_STREAM_NAME)
            .extendedS3DestinationConfiguration(EXTENDED_S3_DESTINATION_CONFIGURATION_FULL)
            .build();
        final ResourceModel previousModel = ResourceModel.builder()
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
        TestHelpers.stubDescribeDeliveryStreamWithProvidedOrEmptyResponse(proxy, describeResponse);
        TestHelpers.stubUpdateDestinationWithProvidedOrEmptyResponse(proxy, updateResponse);
        doThrow(FirehoseException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ServerSideErrorOccurred").build())
            .build()).when(proxy)
            .injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(previousModel)
            .previousResourceTags(PREVIOUS_CFN_MODEL_TAGS_IN_MAP)
            .desiredResourceTags(CFN_MODEL_TAGS_IN_MAP)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response
            = handler.handleRequest(proxy, request, null, logger);
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UpdateDestinationRequest.class), any());
        verify(proxy, times(1)).injectCredentialsAndInvokeV2(any(UntagDeliveryStreamRequest.class), any());
        verify(proxy, times(0)).injectCredentialsAndInvokeV2(any(TagDeliveryStreamRequest.class), any());
    }
}
