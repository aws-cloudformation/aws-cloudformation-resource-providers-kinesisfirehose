package com.amazonaws.kinesisfirehose.deliverystream;

import com.google.common.collect.ImmutableMap;

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
    public static final String DELIVERY_STREAM_TYPE_UPDATED = "streamTypeUpdated";

    public static final ResourceModel RESOURCE_UNNAMED = new ResourceModel(null, null, null, null, null, null,  null, null, S3_DESTINATION_CONFIG, null);
    public static final ResourceModel RESOURCE = new ResourceModel(DELIVERY_STREAM_NAME, null, DELIVERY_STREAM_NAME, null, null, null, null, null, S3_DESTINATION_CONFIG, null);
    public static final ResourceModel RESOURCE_UPDATED = new ResourceModel(DELIVERY_STREAM_NAME, null, DELIVERY_STREAM_NAME, null, null, null, null, null,  S3_DESTINATION_CONFIG_UPDATED, null);

    public static final CloudWatchLoggingOptions CLOUD_WATCH_LOGGING_OPTIONS = new CloudWatchLoggingOptions(true, "LogGroupName", "LogStreamName");
    public static final ProcessingConfiguration PROCESSING_CONFIGURATION = new ProcessingConfiguration(true, Collections.emptyList());
    public static final SplunkRetryOptions RETRY_OPTIONS = new SplunkRetryOptions(INTERVAL_IN_SECONDS);
    public static final SplunkDestinationConfiguration SPLUNK_CONFIGURATION_FULL = new SplunkDestinationConfiguration(CLOUD_WATCH_LOGGING_OPTIONS, 60, "endpoint", "type", "token", PROCESSING_CONFIGURATION, RETRY_OPTIONS, "backup", S3_DESTINATION_CONFIG);
    public static final ResourceModel RESOURCE_WITH_SPLUNK = new ResourceModel(null, null, null, null, null, null, null, null, null, SPLUNK_CONFIGURATION_FULL);

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
    public static final ResourceModel RESOURCE_WITH_S3_EXTENDED = new ResourceModel(DELIVERY_STREAM_NAME, DELIVERY_STREAM_NAME_ARN, DELIVERY_STREAM_NAME, DELIVERY_STREAM_TYPE, null, EXTENDED_S3_DESTINATION_CONFIGURATION_FULL, null, null, null,  null);


    public static final String DOMAIN_ARN = "DomainArn";
    public static final String INDEX_NAME = "IndexName";
    public static final String INDEX_ROTATION_PERIOD = "RotationPeriod";
    public static final String TYPE_NAME = "TypeName";
    public static final int DURATION_IN_SECONDS = 120;
    public static final ElasticsearchRetryOptions ELASTICSEARCH_RETRY_OPTIONS = new ElasticsearchRetryOptions(DURATION_IN_SECONDS);
    public static final ElasticsearchBufferingHints ELASTICSEARCH_BUFFERING_HINTS = new ElasticsearchBufferingHints(INTERVAL_IN_SECONDS, SIZE_IN_MBS);
    public static final ElasticsearchDestinationConfiguration ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL = new ElasticsearchDestinationConfiguration(ELASTICSEARCH_BUFFERING_HINTS, CLOUD_WATCH_LOGGING_OPTIONS, DOMAIN_ARN, INDEX_NAME, INDEX_ROTATION_PERIOD, PROCESSING_CONFIGURATION, ELASTICSEARCH_RETRY_OPTIONS, ROLE_ARN, BACKUP_MODE, S3_DESTINATION_CONFIG_FULL, TYPE_NAME);
    public static final ResourceModel RESOURCE_WITH_ELASTICSEARCH = new ResourceModel(DELIVERY_STREAM_NAME, DELIVERY_STREAM_NAME_ARN, DELIVERY_STREAM_NAME, DELIVERY_STREAM_TYPE, ELASTICSEARCH_DESTINATION_CONFIGURATION_FULL,null, null, null, null,  null);

    public static final String KINESIS_STREAM_ARN = "KinesisStreamArn";
    public static final KinesisStreamSourceConfiguration KINESIS_STREAM_SOURCE_CONFIGURATION = new KinesisStreamSourceConfiguration(KINESIS_STREAM_ARN, ROLE_ARN);
    public static final ResourceModel RESOURCE_WITH_KINESIS = new ResourceModel(DELIVERY_STREAM_NAME, DELIVERY_STREAM_NAME_ARN, DELIVERY_STREAM_NAME, DELIVERY_STREAM_TYPE, null, EXTENDED_S3_DESTINATION_CONFIGURATION_FULL, KINESIS_STREAM_SOURCE_CONFIGURATION, null,  null, null);

    public static final RedshiftDestinationConfiguration REDSHIFT_DESTINATION_CONFIGURATION = new RedshiftDestinationConfiguration(CLOUD_WATCH_LOGGING_OPTIONS, "ClusterJBDCurl", new CopyCommand("CopyOptions", "DataTableColumns", "DataTableName"), "Password", PROCESSING_CONFIGURATION, ROLE_ARN, S3_DESTINATION_CONFIG_FULL, "Username");
    public static final ResourceModel RESOURCE_WITH_REDSHIFT = new ResourceModel(DELIVERY_STREAM_NAME, DELIVERY_STREAM_NAME_ARN, DELIVERY_STREAM_NAME, DELIVERY_STREAM_TYPE, null, null, null, REDSHIFT_DESTINATION_CONFIGURATION, null,  null);
}
