package com.amazonaws.kinesisfirehose.deliverystream;


import static com.amazonaws.kinesisfirehose.deliverystream.DeliveryStreamTestHelper.*;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.firehose.model.ContentEncoding;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class HandlerUtilsTest {
    @Test
    public void testTransferHttpEndpointConfig() {
        val endpointConfig = HandlerUtils.translateHttpEndpointConfiguration(HTTP_ENDPOINT_CONFIGURATION);
        validateHttpEndpointWithFullFields(endpointConfig);
        assertThat(HandlerUtils.translateHttpEndpointConfiguration(null)).isNull();
    }

    @Test
    public void testTransferHttpRequestConfig() {
        val request = HandlerUtils.translateHttpEndpointRequestConfiguration(HTTP_ENDPOINT_REQUEST_CONFIGURATION.build());
        validateHttpEndpointRequestConfigWithFullFields(request);
        val requestConfigWithEmptyAttributes = HandlerUtils.translateHttpEndpointRequestConfiguration(
                HTTP_ENDPOINT_REQUEST_CONFIGURATION.commonAttributes(null).build());
        assertThat(requestConfigWithEmptyAttributes.commonAttributes()).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testTranslateHttpEndpointToCfn() {
        val update = HandlerUtils.translateHttpEndpointConfigurationToCfnModel(ENDPOINT_DESCRIPTION);
        validateHttpEndpointWithFullFields(update);

        assertThat(HandlerUtils.translateHttpEndpointConfigurationToCfnModel(null)).isNull();
    }

    @Test
    public void testTranslateHttpRequestToCfn() {
        val update = HandlerUtils.translateHttpEndpointRequestConfigurationToCfnModel(REQUEST_CONFIGURATION);
        validateHttpEndpointRequestConfigWithFullFields(update);

        val updateWithEmptyAttributes = HandlerUtils.translateHttpEndpointRequestConfigurationToCfnModel(software.amazon.awssdk.services.firehose.model.HttpEndpointRequestConfiguration.builder()
                .contentEncoding(ContentEncoding.fromValue(CONTENT_ENCODE))
                .build());

        assertThat(updateWithEmptyAttributes.getCommonAttributes()).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testTranslateHttpEndpointDestinationConfig() {
        val desConfig = HandlerUtils.translateHttpEndpointDestinationConfiguration(HTTP_ENDPOINT_DESTINATION_CONFIGURATION);
        validateHttpEndpointRequestConfigWithFullFields(desConfig.requestConfiguration());
        validateHttpEndpointWithFullFields(desConfig.endpointConfiguration());
        assertThat(desConfig.roleARN()).isEqualTo(ROLE_ARN);
        assertThat(desConfig.bufferingHints()).isEqualToComparingFieldByField(BUFFERING_HINTS);
        assertThat(desConfig.cloudWatchLoggingOptions()).isEqualToComparingFieldByField(CLOUD_WATCH_LOGGING_OPTIONS);
        assertThat(desConfig.processingConfiguration()).isEqualToComparingFieldByField(PROCESSING_CONFIGURATION);
        assertThat(desConfig.retryOptions()).isEqualToComparingFieldByField(COMMON_RETRY_OPTIONS);
        assertThat(desConfig.s3BackupModeAsString()).isEqualTo("AllData");
        assertThat(desConfig.s3Configuration()).isNotNull();
    }

    @Test
    public void testTranslateHttpEndpointDestinationConfigToCfn() {
        val desConfig = HandlerUtils.translateHttpEndpointDestinationConfigurationToCfnModel(HTTP_ENDPOINT_DESTINATION_DESCRIPTION);
        validateHttpEndpointRequestConfigWithFullFields(desConfig.getRequestConfiguration());
        validateHttpEndpointWithFullFields(desConfig.getEndpointConfiguration());
        assertThat(desConfig.getRoleARN()).isEqualTo(ROLE_ARN);
        assertThat(desConfig.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(desConfig.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(desConfig.getCloudWatchLoggingOptions()).isEqualToComparingFieldByField(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE);
        assertThat(desConfig.getRetryOptions().getDurationInSeconds()).isEqualTo(1);
        assertThat(desConfig.getS3BackupMode()).isEqualTo(BACKUP_MODE);
        assertThat(desConfig.getS3Configuration()).isNotNull();
        assertThat(desConfig.getProcessingConfiguration()).isNotNull();
    }

    private void validateHttpEndpointWithFullFields(HttpEndpointConfiguration httpEndpointConfiguration) {
        assertThat(httpEndpointConfiguration.getUrl()).isEqualTo(ENDPOINT_URL);
        assertThat(httpEndpointConfiguration.getName()).isEqualTo(ENDPOINT_NAME);
    }

    private void validateHttpEndpointRequestConfigWithFullFields(HttpEndpointRequestConfiguration requestConfiguration) {
        assertThat(requestConfiguration.getContentEncoding()).isEqualTo(CONTENT_ENCODE);
        assertThat(requestConfiguration.getCommonAttributes().get(0).getAttributeName()).isEqualTo(ATTRIBUTE_NAME);
        assertThat(requestConfiguration.getCommonAttributes().get(0).getAttributeValue()).isEqualTo(ATTRIBUTE_VALUE);
    }

    private void validateHttpEndpointWithFullFields(
            software.amazon.awssdk.services.firehose.model.HttpEndpointConfiguration httpEndpointConfiguration) {
        assertThat(httpEndpointConfiguration.accessKey()).isEqualTo(HTTP_ENDPOINT_CONFIGURATION.getAccessKey());
        assertThat(httpEndpointConfiguration.name()).isEqualTo(HTTP_ENDPOINT_CONFIGURATION.getName());
        assertThat(httpEndpointConfiguration.url()).isEqualTo(HTTP_ENDPOINT_CONFIGURATION.getUrl());
    }

    public void validateHttpEndpointRequestConfigWithFullFields(
            software.amazon.awssdk.services.firehose.model.HttpEndpointRequestConfiguration requestConfiguration) {
        assertThat(requestConfiguration.contentEncoding().toString()).isEqualTo(CONTENT_ENCODE);
        assertThat(requestConfiguration.commonAttributes().get(0).attributeName()).isEqualTo(ATTRIBUTE_NAME);
        assertThat(requestConfiguration.commonAttributes().get(0).attributeValue()).isEqualTo(ATTRIBUTE_VALUE);
    }
}
