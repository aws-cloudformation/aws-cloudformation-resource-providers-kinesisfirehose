package com.amazonaws.kinesisfirehose.deliverystream;

import static com.amazonaws.kinesisfirehose.deliverystream.DeliveryStreamTestHelper.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.firehose.model.*;
import software.amazon.awssdk.services.firehose.model.BufferingHints;
import software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions;
import software.amazon.awssdk.services.firehose.model.CopyCommand;
import software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration;
import software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints;
import software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions;
import software.amazon.awssdk.services.firehose.model.HiveJsonSerDe;
import software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe;
import software.amazon.awssdk.services.firehose.model.ProcessingConfiguration;
import software.amazon.awssdk.services.firehose.model.Processor;
import software.amazon.awssdk.services.firehose.model.ProcessorParameter;
import software.amazon.awssdk.services.firehose.model.SchemaConfiguration;
import software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration;
import software.amazon.awssdk.services.firehose.model.InputFormatConfiguration;
import software.amazon.awssdk.services.firehose.model.Deserializer;
import software.amazon.awssdk.services.firehose.model.Serializer;
import software.amazon.awssdk.services.firehose.model.EncryptionConfiguration;
import software.amazon.awssdk.services.firehose.model.OrcSerDe;
import software.amazon.awssdk.services.firehose.model.ParquetSerDe;
import software.amazon.awssdk.services.firehose.model.SplunkRetryOptions;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import lombok.val;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest {
    private final static CloudWatchLoggingOptions CLOUDWATCH_LOGGING_OPTIONS =
            CloudWatchLoggingOptions.builder().enabled(true).logGroupName("LogGroupName").logStreamName("LogStreamName").build();

    private final static KinesisStreamSourceDescription KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE = KinesisStreamSourceDescription.builder()
            .kinesisStreamARN(KINESIS_STREAM_ARN)
            .roleARN(ROLE_ARN)
            .build();
    private final static S3DestinationDescription S_3_DESTINATION_DESCRIPTION_RESPONSE = S3DestinationDescription.builder()
            .bucketARN(BUCKET_ARN)
            .bufferingHints(
                    BufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
            .compressionFormat(COMPRESSION_FORMAT)
            .encryptionConfiguration(EncryptionConfiguration.builder().noEncryptionConfig(NO_ENCRYPTION_CONFIG).build())
            .errorOutputPrefix(ERROR_OUTPUT_PREFIX)
            .prefix(PREFIX)
            .roleARN(ROLE_ARN)
            .build();

    private final static ProcessingConfiguration PROCESSING_CONFIGURATION = ProcessingConfiguration.builder()
            .enabled(true)
            .processors(Processor.builder()
                    .parameters(ProcessorParameter.builder()
                            .parameterName("name")
                            .parameterValue("value")
                            .build())
                    .type(ProcessorType.LAMBDA)
                    .build())
            .build();

    private final static DataFormatConversionConfiguration DATA_FORMAT_CONVERSION_CONFIGURATION_RESPONSE = DataFormatConversionConfiguration.builder()
            .schemaConfiguration(SchemaConfiguration.builder()
                    .versionId("versionId")
                    .tableName("tableName")
                    .roleARN(ROLE_ARN)
                    .region("us-east-1")
                    .databaseName("databaseName")
                    .catalogId("catelogId")
                    .build())
            .outputFormatConfiguration(OutputFormatConfiguration.builder()
                    .serializer(Serializer.builder()
                            .orcSerDe(OrcSerDe.builder()
                                    .stripeSizeBytes(1)
                                    .rowIndexStride(1)
                                    .paddingTolerance(1D)
                                    .formatVersion("formatVersion")
                                    .enablePadding(true)
                                    .dictionaryKeyThreshold(1D)
                                    .compression(COMPRESSION_FORMAT)
                                    .bloomFilterFalsePositiveProbability(1D)
                                    .bloomFilterColumns(ImmutableList.of("bloomFilterColumns"))
                                    .blockSizeBytes(1)
                                    .build())
                            .parquetSerDe(ParquetSerDe.builder()
                                    .writerVersion("writerVersion")
                                    .pageSizeBytes(1)
                                    .maxPaddingBytes(1)
                                    .enableDictionaryCompression(true)
                                    .compression(COMPRESSION_FORMAT)
                                    .blockSizeBytes(1)
                                    .build())
                            .build())
                    .build())
            .inputFormatConfiguration(InputFormatConfiguration.builder()
                    .deserializer(Deserializer.builder()
                            .hiveJsonSerDe(HiveJsonSerDe.builder()
                                    .timestampFormats("timestampFormats")
                                    .build())
                            .openXJsonSerDe(OpenXJsonSerDe.builder()
                                    .caseInsensitive(true)
                                    .columnToJsonKeyMappings(ImmutableMap.of("key", "value"))
                                    .convertDotsInJsonKeysToUnderscores(true)
                                    .build())
                            .build())
                    .build())
            .enabled(true)
            .build();

    private final static ExtendedS3DestinationDescription.Builder EXTENDED_S_3_DESTINATION_DESCRIPTION_BUILDER = ExtendedS3DestinationDescription.builder()
            .s3BackupDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .s3BackupMode(BACKUP_MODE)
            .bucketARN(BUCKET_ARN)
            .bufferingHints(BufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .compressionFormat(COMPRESSION_FORMAT)
            .errorOutputPrefix(ERROR_OUTPUT_PREFIX)
            .roleARN(ROLE_ARN);

    private final static RedshiftDestinationDescription.Builder REDSHIFT_DESTINATION_DESCRIPTION_BUILDER = RedshiftDestinationDescription.builder()
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .s3BackupDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .clusterJDBCURL("clusterJDBCURL")
            .copyCommand(CopyCommand.builder().copyOptions("copyOptions").dataTableColumns("dataTableColumns").dataTableName("dataTableName").build())
            .retryOptions(RedshiftRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .username("username");

    private final static ElasticsearchDestinationDescription.Builder ELASTICSEARCH_DESTINATION_DESCRIPTION_RESPONSE = ElasticsearchDestinationDescription.builder()
            .bufferingHints(ElasticsearchBufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
            .clusterEndpoint(CLUSTER_END_POINT)
            .domainARN(DOMAIN_ARN)
            .indexName(INDEX_NAME)
            .indexRotationPeriod(INDEX_ROTATION_PERIOD)
            .retryOptions(ElasticsearchRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .typeName(TYPE_NAME);

    private final static SplunkDestinationDescription SPLUNK_DESTINATION_DESCRIPTION_RESPONSE = SplunkDestinationDescription.builder()
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
            .hecAcknowledgmentTimeoutInSeconds(1)
            .hecEndpoint("hecEndpoint")
            .hecEndpointType("hecEndpointType")
            .hecToken("hecToken")
            .processingConfiguration(PROCESSING_CONFIGURATION)
            .retryOptions(SplunkRetryOptions.builder().durationInSeconds(1).build())
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .build();

    private ReadHandler readHandler;

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @BeforeEach
    public void setup() {
        logger = mock(Logger.class);
        readHandler = new ReadHandler();
        System.out.println("Finish read test setup");
    }

    @Test
    public void testReadKinesisStreamAsSource() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .source(SourceDescription.builder().kinesisStreamSourceDescription(KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE).build())
                        .destinations(ImmutableList.of(DestinationDescription.builder().s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);
        val resourceModel = response.getResourceModel();
        val source = resourceModel.getKinesisStreamSourceConfiguration();
        assertThat(source.getKinesisStreamARN()).isEqualTo(KINESIS_STREAM_ARN);
        assertThat(source.getRoleARN()).isEqualTo(ROLE_ARN);
    }

    @Test
    public void testReadExtendedS3DeliveryStream() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder().extendedS3DestinationDescription(EXTENDED_S_3_DESTINATION_DESCRIPTION_BUILDER.build()).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getExtendedS3DestinationConfiguration();
        assertThat(destination.getBucketARN()).isEqualTo(BUCKET_ARN);
        assertThat(destination.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(destination.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(destination.getCompressionFormat()).isEqualTo(COMPRESSION_FORMAT);
        assertThat(destination.getErrorOutputPrefix()).isEqualTo(ERROR_OUTPUT_PREFIX);
        assertThat(destination.getRoleARN()).isEqualTo(ROLE_ARN);
    }

    @Test
    public void testReadExtendedS3DeliveryStreamWithCloudwatchLoggingAndProcessing() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .extendedS3DestinationDescription(EXTENDED_S_3_DESTINATION_DESCRIPTION_BUILDER
                                        .processingConfiguration(PROCESSING_CONFIGURATION)
                                        .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
                                        .build()).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getExtendedS3DestinationConfiguration();
        assertThat(destination.getBucketARN()).isEqualTo(BUCKET_ARN);
        assertThat(destination.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(destination.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(destination.getCompressionFormat()).isEqualTo(COMPRESSION_FORMAT);
        assertThat(destination.getErrorOutputPrefix()).isEqualTo(ERROR_OUTPUT_PREFIX);
        assertThat(destination.getRoleARN()).isEqualTo(ROLE_ARN);
        validateProcessingConfiguration(destination.getProcessingConfiguration());
        validateCloudWatchConfig(destination.getCloudWatchLoggingOptions());
    }

    @Test
    public void testReadRedshiftDestinationConfiguration() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .redshiftDestinationDescription(REDSHIFT_DESTINATION_DESCRIPTION_BUILDER.build()).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getRedshiftDestinationConfiguration();
        assertThat(destination.getClusterJDBCURL()).isEqualTo("clusterJDBCURL");
        assertThat(destination.getUsername()).isEqualTo("username");
        assertThat(destination.getRoleARN()).isEqualTo(ROLE_ARN);
        val copyCommand = destination.getCopyCommand();
        assertThat(copyCommand.getCopyOptions()).isEqualTo("copyOptions");
        assertThat(copyCommand.getDataTableColumns()).isEqualTo("dataTableColumns");
        assertThat(copyCommand.getDataTableName()).isEqualTo("dataTableName");
        validateS3Configuration(destination.getS3Configuration());
    }

    @Test
    public void testReadRedshiftDestinationConfigurationWithProcessingAndCloudwatchLogging() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .redshiftDestinationDescription(REDSHIFT_DESTINATION_DESCRIPTION_BUILDER
                                        .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
                                        .processingConfiguration(PROCESSING_CONFIGURATION)
                                        .build()).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getRedshiftDestinationConfiguration();
        validateCloudWatchConfig(destination.getCloudWatchLoggingOptions());
        validateProcessingConfiguration(destination.getProcessingConfiguration());
    }

    @Test
    public void testReadDataFormatConversion() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .extendedS3DestinationDescription(EXTENDED_S_3_DESTINATION_DESCRIPTION_BUILDER
                                        .dataFormatConversionConfiguration(DATA_FORMAT_CONVERSION_CONFIGURATION_RESPONSE).build()).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getExtendedS3DestinationConfiguration();
        val dataformatConversionConfg = destination.getDataFormatConversionConfiguration();
        assertThat(dataformatConversionConfg.getEnabled()).isEqualTo(true);
        val deserializer = dataformatConversionConfg.getInputFormatConfiguration().getDeserializer();
        assertThat(deserializer.getHiveJsonSerDe().getTimestampFormats()).isEqualTo(ImmutableList.of("timestampFormats"));
        assertThat(deserializer.getOpenXJsonSerDe().getCaseInsensitive()).isEqualTo(true);
        assertThat(deserializer.getOpenXJsonSerDe().getColumnToJsonKeyMappings()).isEqualTo(ImmutableMap.of("key", "value"));
        assertThat(deserializer.getOpenXJsonSerDe().getConvertDotsInJsonKeysToUnderscores()).isEqualTo(true);
        val serializer = dataformatConversionConfg.getOutputFormatConfiguration().getSerializer();
        assertThat(serializer.getOrcSerDe().getBlockSizeBytes()).isEqualTo(1);
        assertThat(serializer.getOrcSerDe().getBloomFilterColumns()).isEqualTo(ImmutableList.of("bloomFilterColumns"));
        assertThat(serializer.getOrcSerDe().getBloomFilterFalsePositiveProbability()).isEqualTo(1D);
        assertThat(serializer.getOrcSerDe().getCompression()).isEqualTo(COMPRESSION_FORMAT);
        assertThat(serializer.getOrcSerDe().getDictionaryKeyThreshold()).isEqualTo(1D);
        assertThat(serializer.getOrcSerDe().getEnablePadding()).isEqualTo(true);
        assertThat(serializer.getOrcSerDe().getFormatVersion()).isEqualTo("formatVersion");
        assertThat(serializer.getOrcSerDe().getPaddingTolerance()).isEqualTo(1D);
        assertThat(serializer.getOrcSerDe().getRowIndexStride()).isEqualTo(1);
        assertThat(serializer.getOrcSerDe().getStripeSizeBytes()).isEqualTo(1);
        val schema = dataformatConversionConfg.getSchemaConfiguration();
        assertThat(schema.getCatalogId()).isEqualTo("catelogId");
        assertThat(schema.getDatabaseName()).isEqualTo("databaseName");
        assertThat(schema.getRegion()).isEqualTo("us-east-1");
        assertThat(schema.getRoleARN()).isEqualTo(ROLE_ARN);
        assertThat(schema.getTableName()).isEqualTo("tableName");
        assertThat(schema.getVersionId()).isEqualTo("versionId");
    }

    @Test
    public void testReadS3DeliveryStream() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .destinations(ImmutableList.of(DestinationDescription.builder().s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE).build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);

        val resourceModel = response.getResourceModel();
        assertThat(resourceModel.getDeliveryStreamName()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getId()).isEqualTo(DELIVERY_STREAM_NAME);
        assertThat(resourceModel.getDeliveryStreamType()).isEqualTo(DeliveryStreamStatus.ACTIVE.toString());
        val destination = resourceModel.getS3DestinationConfiguration();
        assertThat(destination.getBucketARN()).isEqualTo(BUCKET_ARN);
        assertThat(destination.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(destination.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(destination.getCompressionFormat()).isEqualTo(COMPRESSION_FORMAT);
        assertThat(destination.getEncryptionConfiguration().getNoEncryptionConfig()).isEqualTo(NO_ENCRYPTION_CONFIG);
        assertThat(destination.getErrorOutputPrefix()).isEqualTo(ERROR_OUTPUT_PREFIX);
        assertThat(destination.getPrefix()).isEqualTo(PREFIX);
        assertThat(destination.getRoleARN()).isEqualTo(ROLE_ARN);
        validateCloudWatchConfig(destination.getCloudWatchLoggingOptions());
    }

    @Test
    public void testReadElasticsearchConfiguration() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .source(SourceDescription.builder().kinesisStreamSourceDescription(KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE).build())
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .elasticsearchDestinationDescription(ELASTICSEARCH_DESTINATION_DESCRIPTION_RESPONSE.build())
                                .build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);

        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);
        val resourceModel = response.getResourceModel();
        val esConfig = resourceModel.getElasticsearchDestinationConfiguration();
        assertThat(esConfig.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(esConfig.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(esConfig.getClusterEndpoint()).isEqualTo(CLUSTER_END_POINT);
        assertThat(esConfig.getDomainARN()).isEqualTo(DOMAIN_ARN);
        assertThat(esConfig.getIndexName()).isEqualTo(INDEX_NAME);
        assertThat(esConfig.getIndexRotationPeriod()).isEqualTo(INDEX_ROTATION_PERIOD);
        assertThat(esConfig.getRetryOptions().getDurationInSeconds()).isEqualTo(1);
        assertThat(esConfig.getRoleARN()).isEqualTo(ROLE_ARN);
        assertThat(esConfig.getS3BackupMode()).isEqualTo(BACKUP_MODE);
        assertThat(esConfig.getTypeName()).isEqualTo(TYPE_NAME);
        assertThat(esConfig.getProcessingConfiguration() == null).isEqualTo(true);
        validateS3Configuration(esConfig.getS3Configuration());
    }

    @Test
    public void testReadElasticsearchConfigurationWithProcessing() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .source(SourceDescription.builder().kinesisStreamSourceDescription(KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE).build())
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .elasticsearchDestinationDescription(ELASTICSEARCH_DESTINATION_DESCRIPTION_RESPONSE
                                        .processingConfiguration(PROCESSING_CONFIGURATION)
                                        .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS)
                                        .build())
                                .build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);

        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);
        val resourceModel = response.getResourceModel();
        val esConfig = resourceModel.getElasticsearchDestinationConfiguration();
        validateProcessingConfiguration(esConfig.getProcessingConfiguration());
        validateCloudWatchConfig(esConfig.getCloudWatchLoggingOptions());
    }

    @Test
    public void testReadSplunkConfiguration() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .source(SourceDescription.builder().kinesisStreamSourceDescription(KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE).build())
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .splunkDestinationDescription(SPLUNK_DESTINATION_DESCRIPTION_RESPONSE)
                                .build()))
                        .build())
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenReturn(describeResponse);
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);
        val resourceModel = response.getResourceModel();
        val splunkConfig = resourceModel.getSplunkDestinationConfiguration();

        assertThat(splunkConfig.getHECAcknowledgmentTimeoutInSeconds()).isEqualTo(1);
        assertThat(splunkConfig.getHECEndpoint()).isEqualTo("hecEndpoint");
        assertThat(splunkConfig.getHECEndpointType()).isEqualTo("hecEndpointType");
        assertThat(splunkConfig.getHECToken()).isEqualTo("hecToken");
        assertThat(splunkConfig.getRetryOptions().getDurationInSeconds()).isEqualTo(1);
        assertThat(splunkConfig.getS3BackupMode()).isEqualTo(BACKUP_MODE);

        validateCloudWatchConfig(splunkConfig.getCloudWatchLoggingOptions());
        validateProcessingConfiguration(splunkConfig.getProcessingConfiguration());
        validateS3Configuration(splunkConfig.getS3Configuration());
    }

    @Test
    public void testResourceNotFound() {
        ResourceModel model = ResourceModel.builder().deliveryStreamName(DELIVERY_STREAM_NAME).build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        final DescribeDeliveryStreamResponse describeResponse = DescribeDeliveryStreamResponse.builder()
                .deliveryStreamDescription(DeliveryStreamDescription.builder()
                        .deliveryStreamStatus(DeliveryStreamStatus.ACTIVE)
                        .deliveryStreamARN(DELIVERY_STREAM_NAME_ARN)
                        .deliveryStreamName(DELIVERY_STREAM_NAME)
                        .deliveryStreamType(DELIVERY_STREAM_TYPE)
                        .source(SourceDescription.builder().kinesisStreamSourceDescription(KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE).build())
                        .destinations(ImmutableList.of(DestinationDescription.builder()
                                .splunkDestinationDescription(SPLUNK_DESTINATION_DESCRIPTION_RESPONSE)
                                .build()))
                        .build())
                .build();
        when(proxy.injectCredentialsAndInvokeV2(any(DescribeDeliveryStreamRequest.class), any()))
                .thenThrow(ResourceNotFoundException.builder().message("ResourceNotFound").build());
        val response = new ReadHandler().handleRequest(
                proxy, request, null, logger);
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    private void validateS3Configuration(com.amazonaws.kinesisfirehose.deliverystream.S3DestinationConfiguration s3DestinationConfiguration) {
        assertThat(s3DestinationConfiguration.getBucketARN()).isEqualTo(BUCKET_ARN);
        assertThat(s3DestinationConfiguration.getBufferingHints().getIntervalInSeconds()).isEqualTo(INTERVAL_IN_SECONDS);
        assertThat(s3DestinationConfiguration.getBufferingHints().getSizeInMBs()).isEqualTo(SIZE_IN_MBS);
        assertThat(s3DestinationConfiguration.getCompressionFormat()).isEqualTo(COMPRESSION_FORMAT);
        assertThat(s3DestinationConfiguration.getEncryptionConfiguration().getNoEncryptionConfig()).isEqualTo(NO_ENCRYPTION_CONFIG);
        assertThat(s3DestinationConfiguration.getErrorOutputPrefix()).isEqualTo(ERROR_OUTPUT_PREFIX);
        assertThat(s3DestinationConfiguration.getPrefix()).isEqualTo(PREFIX);
        assertThat(s3DestinationConfiguration.getRoleARN()).isEqualTo(ROLE_ARN);
        validateCloudWatchConfig(s3DestinationConfiguration.getCloudWatchLoggingOptions());
    }

    private void validateProcessingConfiguration(com.amazonaws.kinesisfirehose.deliverystream.ProcessingConfiguration processingConfiguration) {
        assertThat(processingConfiguration.getEnabled()).isEqualTo(true);
        val processor = processingConfiguration.getProcessors().get(0);
        assertThat(processor.getType()).isEqualTo(ProcessorType.LAMBDA.toString());
        assertThat(processor.getParameters().get(0).getParameterName()).isEqualTo("name");
        assertThat(processor.getParameters().get(0).getParameterValue()).isEqualTo("value");
    }

    private void validateCloudWatchConfig(com.amazonaws.kinesisfirehose.deliverystream.CloudWatchLoggingOptions cloudWatchLoggingOptions) {
        assertThat(cloudWatchLoggingOptions.getEnabled()).isEqualTo(true);
        assertThat(cloudWatchLoggingOptions.getLogGroupName()).isEqualTo("LogGroupName");
        assertThat(cloudWatchLoggingOptions.getLogStreamName()).isEqualTo("LogStreamName");
    }
}
