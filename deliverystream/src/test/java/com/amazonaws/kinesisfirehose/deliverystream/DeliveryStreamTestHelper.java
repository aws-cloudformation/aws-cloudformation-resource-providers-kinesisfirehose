package com.amazonaws.kinesisfirehose.deliverystream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import software.amazon.awssdk.services.firehose.model.*;
import software.amazon.awssdk.services.firehose.model.HiveJsonSerDe;
import software.amazon.awssdk.services.firehose.model.OrcSerDe;
import software.amazon.awssdk.services.firehose.model.Processor;
import software.amazon.awssdk.services.firehose.model.ProcessorParameter;
import com.amazonaws.kinesisfirehose.deliverystream.HttpEndpointRequestConfiguration.HttpEndpointRequestConfigurationBuilder;

import java.util.Collections;

public class DeliveryStreamTestHelper  {
    public static final int INTERVAL_IN_SECONDS = 120;
    public static final int SIZE_IN_MBS = 100;
    public static final int INTERVAL_IN_SECONDS_UPDATED = 60;
    public static final int SIZE_IN_MBS_UPDATED = 200;
    public static final BufferingHints BUFFERING_HINTS = new BufferingHints(SIZE_IN_MBS, INTERVAL_IN_SECONDS);
    public static final BufferingHints BUFFERING_HINTS_UPDATED = new BufferingHints(SIZE_IN_MBS_UPDATED, INTERVAL_IN_SECONDS_UPDATED);
    public static final String COMPRESSION_FORMAT = "UNCOMPRESSED";
    public static final String COMPRESSION_FORMAT_UPDATED = "GZIP";
    public static final String PREFIX = "prefix/";
    public static final String PREFIX_UPDATED = "prefix_updated/";
    public static final String ROLE_ARN = "ROLE_ARN";
    public static final String ROLE_ARN_UPDATED = "ROLE_ARN_UPDATED";
    public static final String BUCKET_ARN = "BUCKET_ARN";
    public static final String BUCKET_ARN_UPDATED = "BUCKET_ARN_UPDATED";
    public static final String BACKUP_MODE = "BackupMode";
    public static final String KMS_KEY_ARN = "Arn";
    public static final String NO_ENCRYPTION_CONFIG = "NoEncryptionConfig";
    public static final String ERROR_OUTPUT_PREFIX = "error_output_prefix";
    public static final String ERROR_OUTPUT_PREFIX_UPDATE = "error_output_prefix_update";
    public static final KMSEncryptionConfig KMS_ENCRYPTION_CONFIG = new KMSEncryptionConfig(KMS_KEY_ARN);
    public static final EncryptionConfiguration ENCRYPTION_CONFIGURATION = new EncryptionConfiguration(KMS_ENCRYPTION_CONFIG, NO_ENCRYPTION_CONFIG);
    public static final S3DestinationConfiguration S3_DESTINATION_CONFIG = new S3DestinationConfiguration(BUCKET_ARN, BUFFERING_HINTS, null, COMPRESSION_FORMAT, null, PREFIX, ROLE_ARN, ERROR_OUTPUT_PREFIX);
    public static final S3DestinationConfiguration S3_DESTINATION_CONFIG_UPDATED = new S3DestinationConfiguration(BUCKET_ARN_UPDATED, BUFFERING_HINTS_UPDATED, null, COMPRESSION_FORMAT_UPDATED, null, PREFIX_UPDATED, ROLE_ARN_UPDATED, ERROR_OUTPUT_PREFIX_UPDATE);
    public static final String DELIVERY_STREAM_NAME = "streamname";
    public static final String DELIVERY_STREAM_NAME_UPDATED = "streamname_update";
    public static final String DELIVERY_STREAM_NAME_ARN = "arn:aws:firehose:us-east-1:900582091538:deliverystream/" + DELIVERY_STREAM_NAME;
    public static final String DELIVERY_STREAM_NAME_ARN_UPDATED = "arn:aws:firehose:us-east-1:900582091538:deliverystream/" + DELIVERY_STREAM_NAME_UPDATED;
    public static final String DELIVERY_STREAM_TYPE = "streamType";
    public static final String ATTRIBUTE_NAME = "NAME";
    public static final String ATTRIBUTE_VALUE= "VALUE";
    public static final String ENDPOINT_URL = "https://test.com";
    public static final String ACCESS_KEY = "ACCESS_KEY";
    public static final String ENDPOINT_NAME = "NAME";
    public static final String CONTENT_ENCODE = "NONE";
    public static final String DELIVERY_STREAM_KEY_ARN = "DELIVERY_STREAM_KEY_ARN";
    public static final String DELIVERY_STREAM_KEY_TYPE = "DELIVERY_STREAM_KEY_TYPE";


    public static final CloudWatchLoggingOptions CLOUD_WATCH_LOGGING_OPTIONS = new CloudWatchLoggingOptions(true, "LogGroupName", "LogStreamName");
    public static final ProcessingConfiguration PROCESSING_CONFIGURATION = new ProcessingConfiguration(true, Collections.emptyList());
    public static final SplunkRetryOptions RETRY_OPTIONS = new SplunkRetryOptions(INTERVAL_IN_SECONDS);
    public static final RetryOptions COMMON_RETRY_OPTIONS = new RetryOptions(INTERVAL_IN_SECONDS);
    public static final SplunkDestinationConfiguration SPLUNK_CONFIGURATION_FULL = new SplunkDestinationConfiguration(CLOUD_WATCH_LOGGING_OPTIONS, 60, "endpoint", "type", "token", PROCESSING_CONFIGURATION, RETRY_OPTIONS, "backup", S3_DESTINATION_CONFIG);
    public static final DataFormatConversionConfiguration DATA_FORMAT_CONVERSION_CONFIGURATION = DataFormatConversionConfiguration.builder()
            .inputFormatConfiguration(InputFormatConfiguration.builder()
                    .deserializer(Deserializer.builder()
                            .openXJsonSerDe(OpenXJsonSerDe.builder()
                                    .caseInsensitive(true)
                                    .convertDotsInJsonKeysToUnderscores(false)
                                    .columnToJsonKeyMappings(ImmutableMap.of())
                                    .build())
                            .build())
                    .build())
            .outputFormatConfiguration(OutputFormatConfiguration.builder()
                    .serializer(Serializer.builder()
                            .parquetSerDe(ParquetSerDe.builder()
                                    .build())
                            .build())
                    .build())
            .schemaConfiguration(SchemaConfiguration.builder()
                    .databaseName("SAMPLEDATABASE")
                    .tableName("SAMPLETABLE")
                    .region("us-east-1")
                    .roleARN("SAMPLEROLE")
                    .catalogId("900582091538")
                    .versionId("0")
                    .build())
            .enabled(true)
            .build();

    public static final S3DestinationConfiguration S3_DESTINATION_CONFIG_FULL = new S3DestinationConfiguration(BUCKET_ARN, BUFFERING_HINTS, CLOUD_WATCH_LOGGING_OPTIONS, COMPRESSION_FORMAT, ENCRYPTION_CONFIGURATION, PREFIX, ROLE_ARN, ERROR_OUTPUT_PREFIX);
    public static final ExtendedS3DestinationConfiguration EXTENDED_S3_DESTINATION_CONFIGURATION_FULL = new ExtendedS3DestinationConfiguration(BUCKET_ARN, BUFFERING_HINTS, CLOUD_WATCH_LOGGING_OPTIONS, COMPRESSION_FORMAT, DATA_FORMAT_CONVERSION_CONFIGURATION, ENCRYPTION_CONFIGURATION, ERROR_OUTPUT_PREFIX, PREFIX , PROCESSING_CONFIGURATION, ROLE_ARN, S3_DESTINATION_CONFIG_FULL, BACKUP_MODE);

    public static final String DOMAIN_ARN = "DomainArn";
    public static final String INDEX_NAME = "IndexName";
    public static final String INDEX_ROTATION_PERIOD = "RotationPeriod";
    public static final String CLUSTER_END_POINT = "ClusterEndPoint";
    public static final String TYPE_NAME = "TypeName";
    public static final int DURATION_IN_SECONDS = 120;
    public static final VpcConfiguration VPC_CONFIGURATION = VpcConfiguration.builder()
            .roleARN(ROLE_ARN)
            .securityGroupIds(ImmutableList.of("securityGroupIds"))
            .subnetIds(ImmutableList.of("subnetIds"))
            .build();
    public static final ElasticsearchRetryOptions ELASTICSEARCH_RETRY_OPTIONS = new ElasticsearchRetryOptions(DURATION_IN_SECONDS);
    public static final ElasticsearchBufferingHints ELASTICSEARCH_BUFFERING_HINTS = new ElasticsearchBufferingHints(INTERVAL_IN_SECONDS, SIZE_IN_MBS);
    public static final ElasticsearchDestinationConfiguration ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL = new ElasticsearchDestinationConfiguration(
            ELASTICSEARCH_BUFFERING_HINTS,
            CLOUD_WATCH_LOGGING_OPTIONS,
            DOMAIN_ARN,
            INDEX_NAME,
            INDEX_ROTATION_PERIOD,
            PROCESSING_CONFIGURATION,
            ELASTICSEARCH_RETRY_OPTIONS,
            ROLE_ARN,
            BACKUP_MODE,
            S3_DESTINATION_CONFIG_FULL,
            CLUSTER_END_POINT,
            TYPE_NAME,
            null);
    public static final ElasticsearchDestinationConfiguration ELASTICSEARCH_DESTINATION_CONFIGURATION_VPC = new ElasticsearchDestinationConfiguration(
            ELASTICSEARCH_BUFFERING_HINTS,
            CLOUD_WATCH_LOGGING_OPTIONS,
            DOMAIN_ARN,
            INDEX_NAME,
            INDEX_ROTATION_PERIOD,
            PROCESSING_CONFIGURATION,
            ELASTICSEARCH_RETRY_OPTIONS,
            ROLE_ARN, BACKUP_MODE,
            S3_DESTINATION_CONFIG_FULL,
            CLUSTER_END_POINT,
            TYPE_NAME,
            VPC_CONFIGURATION);
    public static final HttpEndpointConfiguration HTTP_ENDPOINT_CONFIGURATION = HttpEndpointConfiguration.builder()
            .url(ENDPOINT_URL)
            .accessKey(ACCESS_KEY)
            .name(ENDPOINT_NAME)
            .build();
    public static final HttpEndpointRequestConfigurationBuilder HTTP_ENDPOINT_REQUEST_CONFIGURATION = HttpEndpointRequestConfiguration.builder()
            .commonAttributes(ImmutableList.of(HttpEndpointCommonAttribute.builder()
                            .attributeName(ATTRIBUTE_NAME)
                            .attributeValue(ATTRIBUTE_VALUE)
                            .build()))
            .contentEncoding(CONTENT_ENCODE);
    public static final HttpEndpointDestinationConfiguration HTTP_ENDPOINT_DESTINATION_CONFIGURATION = HttpEndpointDestinationConfiguration.builder()
            .requestConfiguration(HTTP_ENDPOINT_REQUEST_CONFIGURATION.build())
            .endpointConfiguration(HTTP_ENDPOINT_CONFIGURATION)
            .roleARN(ROLE_ARN)
            .bufferingHints(BUFFERING_HINTS)
            .cloudWatchLoggingOptions(CLOUD_WATCH_LOGGING_OPTIONS)
            .processingConfiguration(PROCESSING_CONFIGURATION)
            .retryOptions(COMMON_RETRY_OPTIONS)
            .s3BackupMode("AllData")
            .s3Configuration(S3_DESTINATION_CONFIG)
            .build();
    public static final String KINESIS_STREAM_ARN = "KinesisStreamArn";
    public static final KinesisStreamSourceConfiguration KINESIS_STREAM_SOURCE_CONFIGURATION = new KinesisStreamSourceConfiguration(KINESIS_STREAM_ARN, ROLE_ARN);
    public static final RedshiftRetryOptions REDSHIFT_RETRY_OPTIONS = new RedshiftRetryOptions(DURATION_IN_SECONDS);
    public static final RedshiftDestinationConfiguration REDSHIFT_DESTINATION_CONFIGURATION = new RedshiftDestinationConfiguration(
            CLOUD_WATCH_LOGGING_OPTIONS,
            "ClusterJBDCurl",
            new CopyCommand("CopyOptions", "DataTableColumns", "DataTableName"),
            "Password",
            PROCESSING_CONFIGURATION,
            REDSHIFT_RETRY_OPTIONS,
            ROLE_ARN,
            S3_DESTINATION_CONFIG_FULL,
            BACKUP_MODE,
            S3_DESTINATION_CONFIG_FULL,
            "Username");
    public final static software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions CLOUDWATCH_LOGGING_OPTIONS_RESPONSE =
            software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions.builder().enabled(true).logGroupName("LogGroupName").logStreamName("LogStreamName").build();

    public final static KinesisStreamSourceDescription KINESIS_STREAM_SOURCE_DESCRIPTION_RESPONSE = KinesisStreamSourceDescription.builder()
            .kinesisStreamARN(KINESIS_STREAM_ARN)
            .roleARN(ROLE_ARN)
            .build();
    public final static S3DestinationDescription S_3_DESTINATION_DESCRIPTION_RESPONSE = S3DestinationDescription.builder()
            .bucketARN(BUCKET_ARN)
            .bufferingHints(
                    software.amazon.awssdk.services.firehose.model.BufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE)
            .compressionFormat(COMPRESSION_FORMAT)
            .encryptionConfiguration(software.amazon.awssdk.services.firehose.model.EncryptionConfiguration.builder().noEncryptionConfig(NO_ENCRYPTION_CONFIG).build())
            .errorOutputPrefix(ERROR_OUTPUT_PREFIX)
            .prefix(PREFIX)
            .roleARN(ROLE_ARN)
            .build();

    public final static software.amazon.awssdk.services.firehose.model.ProcessingConfiguration PROCESSING_CONFIGURATION_RESPONSE = software.amazon.awssdk.services.firehose.model.ProcessingConfiguration.builder()
            .enabled(true)
            .processors(Processor.builder()
                    .parameters(ProcessorParameter.builder()
                            .parameterName("name")
                            .parameterValue("value")
                            .build())
                    .type(ProcessorType.LAMBDA)
                    .build())
            .build();

    public final static software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration DATA_FORMAT_CONVERSION_CONFIGURATION_RESPONSE = software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration.builder()
            .schemaConfiguration(software.amazon.awssdk.services.firehose.model.SchemaConfiguration.builder()
                    .versionId("versionId")
                    .tableName("tableName")
                    .roleARN(ROLE_ARN)
                    .region("us-east-1")
                    .databaseName("databaseName")
                    .catalogId("catelogId")
                    .build())
            .outputFormatConfiguration(software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration.builder()
                    .serializer(software.amazon.awssdk.services.firehose.model.Serializer.builder()
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
                            .parquetSerDe(software.amazon.awssdk.services.firehose.model.ParquetSerDe.builder()
                                    .writerVersion("writerVersion")
                                    .pageSizeBytes(1)
                                    .maxPaddingBytes(1)
                                    .enableDictionaryCompression(true)
                                    .compression(COMPRESSION_FORMAT)
                                    .blockSizeBytes(1)
                                    .build())
                            .build())
                    .build())
            .inputFormatConfiguration(software.amazon.awssdk.services.firehose.model.InputFormatConfiguration.builder()
                    .deserializer(software.amazon.awssdk.services.firehose.model.Deserializer.builder()
                            .hiveJsonSerDe(HiveJsonSerDe.builder()
                                    .timestampFormats("timestampFormats")
                                    .build())
                            .openXJsonSerDe(software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe.builder()
                                    .caseInsensitive(true)
                                    .columnToJsonKeyMappings(ImmutableMap.of("key", "value"))
                                    .convertDotsInJsonKeysToUnderscores(true)
                                    .build())
                            .build())
                    .build())
            .enabled(true)
            .build();

    public final static ExtendedS3DestinationDescription.Builder EXTENDED_S_3_DESTINATION_DESCRIPTION_BUILDER = ExtendedS3DestinationDescription.builder()
            .s3BackupDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .s3BackupMode(BACKUP_MODE)
            .bucketARN(BUCKET_ARN)
            .bufferingHints(software.amazon.awssdk.services.firehose.model.BufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .compressionFormat(COMPRESSION_FORMAT)
            .errorOutputPrefix(ERROR_OUTPUT_PREFIX)
            .roleARN(ROLE_ARN);

    public final static RedshiftDestinationDescription.Builder REDSHIFT_DESTINATION_DESCRIPTION_BUILDER = RedshiftDestinationDescription.builder()
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .s3BackupDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .clusterJDBCURL("clusterJDBCURL")
            .copyCommand(software.amazon.awssdk.services.firehose.model.CopyCommand.builder().copyOptions("copyOptions").dataTableColumns("dataTableColumns").dataTableName("dataTableName").build())
            .retryOptions(software.amazon.awssdk.services.firehose.model.RedshiftRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .username("username");

    public final static ElasticsearchDestinationDescription.Builder ELASTICSEARCH_DESTINATION_DESCRIPTION_RESPONSE = ElasticsearchDestinationDescription.builder()
            .bufferingHints(software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE)
            .clusterEndpoint(CLUSTER_END_POINT)
            .domainARN(DOMAIN_ARN)
            .indexName(INDEX_NAME)
            .indexRotationPeriod(INDEX_ROTATION_PERIOD)
            .retryOptions(software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .typeName(TYPE_NAME);

    public final static ElasticsearchDestinationDescription.Builder ELASTICSEARCH_DESTINATION_VPC_DESCRIPTION_RESPONSE = ElasticsearchDestinationDescription.builder()
            .bufferingHints(software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE)
            .clusterEndpoint(CLUSTER_END_POINT)
            .domainARN(DOMAIN_ARN)
            .indexName(INDEX_NAME)
            .indexRotationPeriod(INDEX_ROTATION_PERIOD)
            .retryOptions(software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .typeName(TYPE_NAME)
            .vpcConfigurationDescription(VpcConfigurationDescription.builder()
                    .roleARN(ROLE_ARN)
                    .securityGroupIds(ImmutableList.of("securityGroupIds"))
                    .subnetIds(ImmutableList.of("subnetIds"))
                    .build());

    public final static SplunkDestinationDescription SPLUNK_DESTINATION_DESCRIPTION_RESPONSE = SplunkDestinationDescription.builder()
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE)
            .hecAcknowledgmentTimeoutInSeconds(1)
            .hecEndpoint("hecEndpoint")
            .hecEndpointType("hecEndpointType")
            .hecToken("hecToken")
            .processingConfiguration(PROCESSING_CONFIGURATION_RESPONSE)
            .retryOptions(software.amazon.awssdk.services.firehose.model.SplunkRetryOptions.builder().durationInSeconds(1).build())
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .build();

    public final static HttpEndpointDescription ENDPOINT_DESCRIPTION = HttpEndpointDescription.builder()
            .name(ENDPOINT_NAME)
            .url(ENDPOINT_URL)
            .build();

    public final static software.amazon.awssdk.services.firehose.model.HttpEndpointRequestConfiguration REQUEST_CONFIGURATION =
            software.amazon.awssdk.services.firehose.model.HttpEndpointRequestConfiguration.builder()
                    .contentEncoding(ContentEncoding.fromValue(CONTENT_ENCODE))
                    .commonAttributes(ImmutableList.of(software.amazon.awssdk.services.firehose.model.HttpEndpointCommonAttribute.builder()
                            .attributeValue(ATTRIBUTE_VALUE)
                            .attributeName(ATTRIBUTE_NAME)
                            .build()))
                    .build();

    public final static HttpEndpointDestinationDescription HTTP_ENDPOINT_DESTINATION_DESCRIPTION = HttpEndpointDestinationDescription.builder()
            .bufferingHints(HttpEndpointBufferingHints.builder().intervalInSeconds(INTERVAL_IN_SECONDS).sizeInMBs(SIZE_IN_MBS).build())
            .cloudWatchLoggingOptions(CLOUDWATCH_LOGGING_OPTIONS_RESPONSE)
            .endpointConfiguration(ENDPOINT_DESCRIPTION)
            .requestConfiguration(REQUEST_CONFIGURATION)
            .processingConfiguration(PROCESSING_CONFIGURATION_RESPONSE)
            .retryOptions(software.amazon.awssdk.services.firehose.model.HttpEndpointRetryOptions.builder().durationInSeconds(1).build())
            .roleARN(ROLE_ARN)
            .s3BackupMode(BACKUP_MODE)
            .s3DestinationDescription(S_3_DESTINATION_DESCRIPTION_RESPONSE)
            .build();


    public final static DeliveryStreamEncryptionConfigurationInput DELIVERY_STREAM_ENCRYPTION_CONFIGURATION_INPUT =  DeliveryStreamEncryptionConfigurationInput.builder()
        .keyARN(DELIVERY_STREAM_KEY_ARN)
        .keyType(DELIVERY_STREAM_KEY_TYPE)
        .build();

    public final static List<Tag> CFN_MODEL_TAGS =  HandlerUtils.translateFirehoseSDKTagsToCfnModelTags(HandlerUtils.generateNFirehoseTags(5, 3));

    public final static List<software.amazon.awssdk.services.firehose.model.Tag> EXISTING_FIREHOSE_RESPONSE_TAGS = HandlerUtils.generateNFirehoseTags(5, 6);
}
