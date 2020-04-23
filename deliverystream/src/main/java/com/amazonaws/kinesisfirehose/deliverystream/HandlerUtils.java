package com.amazonaws.kinesisfirehose.deliverystream;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import lombok.val;
import software.amazon.awssdk.services.firehose.model.*;

import java.util.Collection;
import java.util.stream.Collectors;

class HandlerUtils {

	static software.amazon.awssdk.services.firehose.model.KinesisStreamSourceConfiguration translateKinesisStreamSourceConfiguration(final KinesisStreamSourceConfiguration kinesisStreamSourceConfiguration) {
		if(kinesisStreamSourceConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.KinesisStreamSourceConfiguration.builder()
				.kinesisStreamARN(kinesisStreamSourceConfiguration.getKinesisStreamARN())
				.roleARN(kinesisStreamSourceConfiguration.getRoleARN())
				.build();
	}

	public static Collection<software.amazon.awssdk.services.firehose.model.KinesisStreamSourceConfiguration> translateKinesisStreamSourceConfigurationCollection(final Collection<KinesisStreamSourceConfiguration> kinesisStreamSourceConfigurationCollection) {
		if(kinesisStreamSourceConfigurationCollection == null) return null;
		return Collections2.transform(kinesisStreamSourceConfigurationCollection, new Function<KinesisStreamSourceConfiguration, software.amazon.awssdk.services.firehose.model.KinesisStreamSourceConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.KinesisStreamSourceConfiguration apply(final KinesisStreamSourceConfiguration input) {
				return translateKinesisStreamSourceConfiguration(input);
			}
		});
	}

	/*public static software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput translateDeliveryStreamEncryptionConfigurationInput(final DeliveryStreamEncryptionConfigurationInput deliveryStreamEncryptionConfigurationInput) {
		if(deliveryStreamEncryptionConfigurationInput == null) return null;
		return software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput.builder()
				.keyType(deliveryStreamEncryptionConfigurationInput.getKeyType())
				.keyARN(deliveryStreamEncryptionConfigurationInput.getKeyARN())
				.build();
	}

	public static Collection<software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput> translateDeliveryStreamEncryptionConfigurationInputCollection(final Collection<DeliveryStreamEncryptionConfigurationInput> deliveryStreamEncryptionConfigurationInputCollection) {
		if(deliveryStreamEncryptionConfigurationInputCollection == null) return null;
		return Collections2.transform(deliveryStreamEncryptionConfigurationInputCollection, new Function<DeliveryStreamEncryptionConfigurationInput, software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput apply(final DeliveryStreamEncryptionConfigurationInput input) {
				return translateDeliveryStreamEncryptionConfigurationInput(input);
			}
		});
	}*/

	static software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration translateS3DestinationConfiguration(final S3DestinationConfiguration s3DestinationConfiguration) {
		if(s3DestinationConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration.builder()
				.bucketARN(s3DestinationConfiguration.getBucketARN())
				.bufferingHints(translateBufferingHints(s3DestinationConfiguration.getBufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(s3DestinationConfiguration.getCloudWatchLoggingOptions()))
				.compressionFormat(s3DestinationConfiguration.getCompressionFormat())
				.encryptionConfiguration(translateEncryptionConfiguration(s3DestinationConfiguration.getEncryptionConfiguration()))
				.prefix(s3DestinationConfiguration.getPrefix())
				.roleARN(s3DestinationConfiguration.getRoleARN())
				.errorOutputPrefix(s3DestinationConfiguration.getErrorOutputPrefix())
				.build();
	}

	public static Collection<software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration> translateS3DestinationConfigurationCollection(final Collection<S3DestinationConfiguration> s3DestinationConfigurationCollection) {
		if(s3DestinationConfigurationCollection == null) return null;
		return Collections2.transform(s3DestinationConfigurationCollection, new Function<S3DestinationConfiguration, software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.S3DestinationConfiguration apply(final S3DestinationConfiguration input) {
				return translateS3DestinationConfiguration(input);
			}
		});
	}

	static software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration translateExtendedS3DestinationConfiguration(final ExtendedS3DestinationConfiguration extendedS3DestinationConfiguration) {
		if(extendedS3DestinationConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration.builder()
				.bucketARN(extendedS3DestinationConfiguration.getBucketARN())
				.bufferingHints(translateBufferingHints(extendedS3DestinationConfiguration.getBufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(extendedS3DestinationConfiguration.getCloudWatchLoggingOptions()))
				.compressionFormat(extendedS3DestinationConfiguration.getCompressionFormat())
				.encryptionConfiguration(translateEncryptionConfiguration(extendedS3DestinationConfiguration.getEncryptionConfiguration()))
				.prefix(extendedS3DestinationConfiguration.getPrefix())
				.roleARN(extendedS3DestinationConfiguration.getRoleARN())
				.processingConfiguration(translateProcessingConfiguration(extendedS3DestinationConfiguration.getProcessingConfiguration()))
				.s3BackupConfiguration(translateS3DestinationConfiguration(extendedS3DestinationConfiguration.getS3BackupConfiguration()))
				.s3BackupMode(extendedS3DestinationConfiguration.getS3BackupMode())
				.errorOutputPrefix(extendedS3DestinationConfiguration.getErrorOutputPrefix())
				.dataFormatConversionConfiguration(translateDataFormatConversionConfiguration(extendedS3DestinationConfiguration.getDataFormatConversionConfiguration()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration> translateExtendedS3DestinationConfigurationCollection(final Collection<ExtendedS3DestinationConfiguration> extendedS3DestinationConfigurationCollection) {
		if(extendedS3DestinationConfigurationCollection == null) return null;
		return Collections2.transform(extendedS3DestinationConfigurationCollection, new Function<ExtendedS3DestinationConfiguration, software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration apply(final ExtendedS3DestinationConfiguration input) {
				return translateExtendedS3DestinationConfiguration(input);
			}
		});
	}

	static software.amazon.awssdk.services.firehose.model.BufferingHints translateBufferingHints(final BufferingHints bufferingHints) {
		if(bufferingHints == null) return null;
		return software.amazon.awssdk.services.firehose.model.BufferingHints.builder()
				.intervalInSeconds(bufferingHints.getIntervalInSeconds())
				.sizeInMBs(bufferingHints.getSizeInMBs())
				.build();
	}

	static Collection<software.amazon.awssdk.services.firehose.model.BufferingHints> translateBufferingHintsCollection(final Collection<BufferingHints> bufferingHintsCollection) {
		if(bufferingHintsCollection == null) return null;
		return Collections2.transform(bufferingHintsCollection, new Function<BufferingHints, software.amazon.awssdk.services.firehose.model.BufferingHints>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.BufferingHints apply(final BufferingHints input) {
				return translateBufferingHints(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.EncryptionConfiguration translateEncryptionConfiguration(final EncryptionConfiguration encryptionConfiguration) {
		if(encryptionConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.EncryptionConfiguration.builder()
				.kmsEncryptionConfig(translateKMSEncryptionConfig(encryptionConfiguration.getKMSEncryptionConfig()))
				.noEncryptionConfig(encryptionConfiguration.getNoEncryptionConfig())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.EncryptionConfiguration> translateEncryptionConfigurationCollection(final Collection<EncryptionConfiguration> encryptionConfigurationCollection) {
		if(encryptionConfigurationCollection == null) return null;
		return Collections2.transform(encryptionConfigurationCollection, new Function<EncryptionConfiguration, software.amazon.awssdk.services.firehose.model.EncryptionConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.EncryptionConfiguration apply(final EncryptionConfiguration input) {
				return translateEncryptionConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.KMSEncryptionConfig translateKMSEncryptionConfig(final KMSEncryptionConfig kMSEncryptionConfig) {
		if(kMSEncryptionConfig == null) return null;
		return software.amazon.awssdk.services.firehose.model.KMSEncryptionConfig.builder()
				.awskmsKeyARN(kMSEncryptionConfig.getAWSKMSKeyARN())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.KMSEncryptionConfig> translateKMSEncryptionConfigCollection(final Collection<KMSEncryptionConfig> kMSEncryptionConfigCollection) {
		if(kMSEncryptionConfigCollection == null) return null;
		return Collections2.transform(kMSEncryptionConfigCollection, new Function<KMSEncryptionConfig, software.amazon.awssdk.services.firehose.model.KMSEncryptionConfig>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.KMSEncryptionConfig apply(final KMSEncryptionConfig input) {
				return translateKMSEncryptionConfig(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions translateCloudWatchLoggingOptions(final CloudWatchLoggingOptions cloudWatchLoggingOptions) {
		if(cloudWatchLoggingOptions == null) return null;
		return software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions.builder()
				.enabled(cloudWatchLoggingOptions.getEnabled())
				.logGroupName(cloudWatchLoggingOptions.getLogGroupName())
				.logStreamName(cloudWatchLoggingOptions.getLogStreamName())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions> translateCloudWatchLoggingOptionsCollection(final Collection<CloudWatchLoggingOptions> cloudWatchLoggingOptionsCollection) {
		if(cloudWatchLoggingOptionsCollection == null) return null;
		return Collections2.transform(cloudWatchLoggingOptionsCollection, new Function<CloudWatchLoggingOptions, software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions apply(final CloudWatchLoggingOptions input) {
				return translateCloudWatchLoggingOptions(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.RedshiftDestinationConfiguration translateRedshiftDestinationConfiguration(final RedshiftDestinationConfiguration redshiftDestinationConfiguration) {
		if(redshiftDestinationConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.RedshiftDestinationConfiguration.builder()
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(redshiftDestinationConfiguration.getCloudWatchLoggingOptions()))
				.clusterJDBCURL(redshiftDestinationConfiguration.getClusterJDBCURL())
				.copyCommand(translateCopyCommand(redshiftDestinationConfiguration.getCopyCommand()))
				.password(redshiftDestinationConfiguration.getPassword())
				.processingConfiguration(translateProcessingConfiguration(redshiftDestinationConfiguration.getProcessingConfiguration()))
				.roleARN(redshiftDestinationConfiguration.getRoleARN())
				.s3Configuration(translateS3DestinationConfiguration(redshiftDestinationConfiguration.getS3Configuration()))
				.username(redshiftDestinationConfiguration.getUsername())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.RedshiftDestinationConfiguration> translateRedshiftDestinationConfigurationCollection(final Collection<RedshiftDestinationConfiguration> redshiftDestinationConfigurationCollection) {
		if(redshiftDestinationConfigurationCollection == null) return null;
		return Collections2.transform(redshiftDestinationConfigurationCollection, new Function<RedshiftDestinationConfiguration, software.amazon.awssdk.services.firehose.model.RedshiftDestinationConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.RedshiftDestinationConfiguration apply(final RedshiftDestinationConfiguration input) {
				return translateRedshiftDestinationConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.CopyCommand translateCopyCommand(final CopyCommand copyCommand) {
		if(copyCommand == null) return null;
		return software.amazon.awssdk.services.firehose.model.CopyCommand.builder()
				.copyOptions(copyCommand.getCopyOptions())
				.dataTableColumns(copyCommand.getDataTableColumns())
				.dataTableName(copyCommand.getDataTableName())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.CopyCommand> translateCopyCommandCollection(final Collection<CopyCommand> copyCommandCollection) {
		if(copyCommandCollection == null) return null;
		return Collections2.transform(copyCommandCollection, new Function<CopyCommand, software.amazon.awssdk.services.firehose.model.CopyCommand>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.CopyCommand apply(final CopyCommand input) {
				return translateCopyCommand(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ElasticsearchDestinationConfiguration translateElasticsearchDestinationConfiguration(final ElasticsearchDestinationConfiguration elasticsearchDestinationConfiguration) {
		if(elasticsearchDestinationConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.ElasticsearchDestinationConfiguration.builder()
				.bufferingHints(translateElasticsearchBufferingHints(elasticsearchDestinationConfiguration.getBufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(elasticsearchDestinationConfiguration.getCloudWatchLoggingOptions()))
				.domainARN(elasticsearchDestinationConfiguration.getDomainARN())
				.indexName(elasticsearchDestinationConfiguration.getIndexName())
				.indexRotationPeriod(elasticsearchDestinationConfiguration.getIndexRotationPeriod())
				.processingConfiguration(translateProcessingConfiguration(elasticsearchDestinationConfiguration.getProcessingConfiguration()))
				.retryOptions(translateElasticsearchRetryOptions(elasticsearchDestinationConfiguration.getRetryOptions()))
				.roleARN(elasticsearchDestinationConfiguration.getRoleARN())
				.s3BackupMode(elasticsearchDestinationConfiguration.getS3BackupMode())
				.s3Configuration(translateS3DestinationConfiguration(elasticsearchDestinationConfiguration.getS3Configuration()))
				.clusterEndpoint(elasticsearchDestinationConfiguration.getClusterEndpoint())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ElasticsearchDestinationConfiguration> translateElasticsearchDestinationConfigurationCollection(final Collection<ElasticsearchDestinationConfiguration> elasticsearchDestinationConfigurationCollection) {
		if(elasticsearchDestinationConfigurationCollection == null) return null;
		return Collections2.transform(elasticsearchDestinationConfigurationCollection, new Function<ElasticsearchDestinationConfiguration, software.amazon.awssdk.services.firehose.model.ElasticsearchDestinationConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ElasticsearchDestinationConfiguration apply(final ElasticsearchDestinationConfiguration input) {
				return translateElasticsearchDestinationConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints translateElasticsearchBufferingHints(final ElasticsearchBufferingHints elasticsearchBufferingHints) {
		if(elasticsearchBufferingHints == null) return null;
		return software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints.builder()
				.intervalInSeconds(elasticsearchBufferingHints.getIntervalInSeconds())
				.sizeInMBs(elasticsearchBufferingHints.getSizeInMBs())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints> translateElasticsearchBufferingHintsCollection(final Collection<ElasticsearchBufferingHints> elasticsearchBufferingHintsCollection) {
		if(elasticsearchBufferingHintsCollection == null) return null;
		return Collections2.transform(elasticsearchBufferingHintsCollection, new Function<ElasticsearchBufferingHints, software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ElasticsearchBufferingHints apply(final ElasticsearchBufferingHints input) {
				return translateElasticsearchBufferingHints(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions translateElasticsearchRetryOptions(final ElasticsearchRetryOptions elasticsearchRetryOptions) {
		if(elasticsearchRetryOptions == null) return null;
		return software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions.builder()
				.durationInSeconds(elasticsearchRetryOptions.getDurationInSeconds())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions> translateElasticsearchRetryOptionsCollection(final Collection<ElasticsearchRetryOptions> elasticsearchRetryOptionsCollection) {
		if(elasticsearchRetryOptionsCollection == null) return null;
		return Collections2.transform(elasticsearchRetryOptionsCollection, new Function<ElasticsearchRetryOptions, software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ElasticsearchRetryOptions apply(final ElasticsearchRetryOptions input) {
				return translateElasticsearchRetryOptions(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ProcessingConfiguration translateProcessingConfiguration(final ProcessingConfiguration processingConfiguration) {
		if(processingConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.ProcessingConfiguration.builder()
				.enabled(processingConfiguration.getEnabled())
				.processors(translateProcessorCollection(processingConfiguration.getProcessors()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ProcessingConfiguration> translateProcessingConfigurationCollection(final Collection<ProcessingConfiguration> processingConfigurationCollection) {
		if(processingConfigurationCollection == null) return null;
		return Collections2.transform(processingConfigurationCollection, new Function<ProcessingConfiguration, software.amazon.awssdk.services.firehose.model.ProcessingConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ProcessingConfiguration apply(final ProcessingConfiguration input) {
				return translateProcessingConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.Processor translateProcessor(final Processor processor) {
		if(processor == null) return null;
		return software.amazon.awssdk.services.firehose.model.Processor.builder()
				.parameters(translateProcessorParameterCollection(processor.getParameters()))
				.type(processor.getType())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.Processor> translateProcessorCollection(final Collection<Processor> processorCollection) {
		if(processorCollection == null) return null;
		return Collections2.transform(processorCollection, new Function<Processor, software.amazon.awssdk.services.firehose.model.Processor>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.Processor apply(final Processor input) {
				return translateProcessor(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ProcessorParameter translateProcessorParameter(final ProcessorParameter processorParameter) {
		if(processorParameter == null) return null;
		return software.amazon.awssdk.services.firehose.model.ProcessorParameter.builder()
				.parameterName(processorParameter.getParameterName())
				.parameterValue(processorParameter.getParameterValue())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ProcessorParameter> translateProcessorParameterCollection(final Collection<ProcessorParameter> processorParameterCollection) {
		if(processorParameterCollection == null) return null;
		return Collections2.transform(processorParameterCollection, new Function<ProcessorParameter, software.amazon.awssdk.services.firehose.model.ProcessorParameter>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ProcessorParameter apply(final ProcessorParameter input) {
				return translateProcessorParameter(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration translateDataFormatConversionConfiguration(final DataFormatConversionConfiguration dataFormatConversionConfiguration) {
		if(dataFormatConversionConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration.builder()
				.schemaConfiguration(translateSchemaConfiguration(dataFormatConversionConfiguration.getSchemaConfiguration()))
				.inputFormatConfiguration(translateInputFormatConfiguration(dataFormatConversionConfiguration.getInputFormatConfiguration()))
				.outputFormatConfiguration(translateOutputFormatConfiguration(dataFormatConversionConfiguration.getOutputFormatConfiguration()))
				.enabled(dataFormatConversionConfiguration.getEnabled())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration> translateDataFormatConversionConfigurationCollection(final Collection<DataFormatConversionConfiguration> dataFormatConversionConfigurationCollection) {
		if(dataFormatConversionConfigurationCollection == null) return null;
		return Collections2.transform(dataFormatConversionConfigurationCollection, new Function<DataFormatConversionConfiguration, software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration apply(final DataFormatConversionConfiguration input) {
				return translateDataFormatConversionConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.SchemaConfiguration translateSchemaConfiguration(final SchemaConfiguration schemaConfiguration) {
		if(schemaConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.SchemaConfiguration.builder()
				.roleARN(schemaConfiguration.getRoleARN())
				.catalogId(schemaConfiguration.getCatalogId())
				.databaseName(schemaConfiguration.getDatabaseName())
				.tableName(schemaConfiguration.getTableName())
				.region(schemaConfiguration.getRegion())
				.versionId(schemaConfiguration.getVersionId())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.SchemaConfiguration> translateSchemaConfigurationCollection(final Collection<SchemaConfiguration> schemaConfigurationCollection) {
		if(schemaConfigurationCollection == null) return null;
		return Collections2.transform(schemaConfigurationCollection, new Function<SchemaConfiguration, software.amazon.awssdk.services.firehose.model.SchemaConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.SchemaConfiguration apply(final SchemaConfiguration input) {
				return translateSchemaConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.InputFormatConfiguration translateInputFormatConfiguration(final InputFormatConfiguration inputFormatConfiguration) {
		if(inputFormatConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.InputFormatConfiguration.builder()
				.deserializer(translateDeserializer(inputFormatConfiguration.getDeserializer()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.InputFormatConfiguration> translateInputFormatConfigurationCollection(final Collection<InputFormatConfiguration> inputFormatConfigurationCollection) {
		if(inputFormatConfigurationCollection == null) return null;
		return Collections2.transform(inputFormatConfigurationCollection, new Function<InputFormatConfiguration, software.amazon.awssdk.services.firehose.model.InputFormatConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.InputFormatConfiguration apply(final InputFormatConfiguration input) {
				return translateInputFormatConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.Deserializer translateDeserializer(final Deserializer deserializer) {
		if(deserializer == null) return null;
		return software.amazon.awssdk.services.firehose.model.Deserializer.builder()
				.openXJsonSerDe(translateOpenXJsonSerDe(deserializer.getOpenXJsonSerDe()))
				.hiveJsonSerDe(translateHiveJsonSerDe(deserializer.getHiveJsonSerDe()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.Deserializer> translateDeserializerCollection(final Collection<Deserializer> deserializerCollection) {
		if(deserializerCollection == null) return null;
		return Collections2.transform(deserializerCollection, new Function<Deserializer, software.amazon.awssdk.services.firehose.model.Deserializer>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.Deserializer apply(final Deserializer input) {
				return translateDeserializer(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration translateOutputFormatConfiguration(final OutputFormatConfiguration outputFormatConfiguration) {
		if(outputFormatConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration.builder()
				.serializer(translateSerializer(outputFormatConfiguration.getSerializer()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration> translateOutputFormatConfigurationCollection(final Collection<OutputFormatConfiguration> outputFormatConfigurationCollection) {
		if(outputFormatConfigurationCollection == null) return null;
		return Collections2.transform(outputFormatConfigurationCollection, new Function<OutputFormatConfiguration, software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration apply(final OutputFormatConfiguration input) {
				return translateOutputFormatConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.Serializer translateSerializer(final Serializer serializer) {
		if(serializer == null) return null;
		return software.amazon.awssdk.services.firehose.model.Serializer.builder()
				.parquetSerDe(translateParquetSerDe(serializer.getParquetSerDe()))
				.orcSerDe(translateOrcSerDe(serializer.getOrcSerDe()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.Serializer> translateSerializerCollection(final Collection<Serializer> serializerCollection) {
		if(serializerCollection == null) return null;
		return Collections2.transform(serializerCollection, new Function<Serializer, software.amazon.awssdk.services.firehose.model.Serializer>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.Serializer apply(final Serializer input) {
				return translateSerializer(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe translateOpenXJsonSerDe(final OpenXJsonSerDe openXJsonSerDe) {
		if(openXJsonSerDe == null) return null;
		return software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe.builder()
				.convertDotsInJsonKeysToUnderscores(openXJsonSerDe.getConvertDotsInJsonKeysToUnderscores())
				.caseInsensitive(openXJsonSerDe.getCaseInsensitive())
				.columnToJsonKeyMappings(openXJsonSerDe.getColumnToJsonKeyMappings())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe> translateOpenXJsonSerDeCollection(final Collection<OpenXJsonSerDe> openXJsonSerDeCollection) {
		if(openXJsonSerDeCollection == null) return null;
		return Collections2.transform(openXJsonSerDeCollection, new Function<OpenXJsonSerDe, software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.OpenXJsonSerDe apply(final OpenXJsonSerDe input) {
				return translateOpenXJsonSerDe(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.HiveJsonSerDe translateHiveJsonSerDe(final HiveJsonSerDe hiveJsonSerDe) {
		if(hiveJsonSerDe == null) return null;
		return software.amazon.awssdk.services.firehose.model.HiveJsonSerDe.builder()
				.timestampFormats(hiveJsonSerDe.getTimestampFormats())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.HiveJsonSerDe> translateHiveJsonSerDeCollection(final Collection<HiveJsonSerDe> hiveJsonSerDeCollection) {
		if(hiveJsonSerDeCollection == null) return null;
		return Collections2.transform(hiveJsonSerDeCollection, new Function<HiveJsonSerDe, software.amazon.awssdk.services.firehose.model.HiveJsonSerDe>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.HiveJsonSerDe apply(final HiveJsonSerDe input) {
				return translateHiveJsonSerDe(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.ParquetSerDe translateParquetSerDe(final ParquetSerDe parquetSerDe) {
		if(parquetSerDe == null) return null;
		return software.amazon.awssdk.services.firehose.model.ParquetSerDe.builder()
				.blockSizeBytes(parquetSerDe.getBlockSizeBytes())
				.pageSizeBytes(parquetSerDe.getPageSizeBytes())
				.compression(parquetSerDe.getCompression())
				.enableDictionaryCompression(parquetSerDe.getEnableDictionaryCompression())
				.maxPaddingBytes(parquetSerDe.getMaxPaddingBytes())
				.writerVersion(parquetSerDe.getWriterVersion())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.ParquetSerDe> translateParquetSerDeCollection(final Collection<ParquetSerDe> parquetSerDeCollection) {
		if(parquetSerDeCollection == null) return null;
		return Collections2.transform(parquetSerDeCollection, new Function<ParquetSerDe, software.amazon.awssdk.services.firehose.model.ParquetSerDe>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.ParquetSerDe apply(final ParquetSerDe input) {
				return translateParquetSerDe(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.OrcSerDe translateOrcSerDe(final OrcSerDe orcSerDe) {
		if(orcSerDe == null) return null;
		return software.amazon.awssdk.services.firehose.model.OrcSerDe.builder()
				.stripeSizeBytes(orcSerDe.getStripeSizeBytes())
				.blockSizeBytes(orcSerDe.getBlockSizeBytes())
				.rowIndexStride(orcSerDe.getRowIndexStride())
				.enablePadding(orcSerDe.getEnablePadding())
				.paddingTolerance(orcSerDe.getPaddingTolerance())
				.compression(orcSerDe.getCompression())
				.bloomFilterColumns(orcSerDe.getBloomFilterColumns())
				.bloomFilterFalsePositiveProbability(orcSerDe.getBloomFilterFalsePositiveProbability())
				.dictionaryKeyThreshold(orcSerDe.getDictionaryKeyThreshold())
				.formatVersion(orcSerDe.getFormatVersion())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.OrcSerDe> translateOrcSerDeCollection(final Collection<OrcSerDe> orcSerDeCollection) {
		if(orcSerDeCollection == null) return null;
		return Collections2.transform(orcSerDeCollection, new Function<OrcSerDe, software.amazon.awssdk.services.firehose.model.OrcSerDe>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.OrcSerDe apply(final OrcSerDe input) {
				return translateOrcSerDe(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.SplunkDestinationConfiguration translateSplunkDestinationConfiguration(final SplunkDestinationConfiguration splunkDestinationConfiguration) {
		if(splunkDestinationConfiguration == null) return null;
		return software.amazon.awssdk.services.firehose.model.SplunkDestinationConfiguration.builder()
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(splunkDestinationConfiguration.getCloudWatchLoggingOptions()))
				.hecAcknowledgmentTimeoutInSeconds(splunkDestinationConfiguration.getHECAcknowledgmentTimeoutInSeconds())
				.hecEndpoint(splunkDestinationConfiguration.getHECEndpoint())
				.hecEndpointType(splunkDestinationConfiguration.getHECEndpointType())
				.hecToken(splunkDestinationConfiguration.getHECToken())
				.processingConfiguration(translateProcessingConfiguration(splunkDestinationConfiguration.getProcessingConfiguration()))
				.retryOptions(translateSplunkRetryOptions(splunkDestinationConfiguration.getRetryOptions()))
				.s3BackupMode(splunkDestinationConfiguration.getS3BackupMode())
				.s3Configuration(translateS3DestinationConfiguration(splunkDestinationConfiguration.getS3Configuration()))
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.SplunkDestinationConfiguration> translateSplunkDestinationConfigurationCollection(final Collection<SplunkDestinationConfiguration> splunkDestinationConfigurationCollection) {
		if(splunkDestinationConfigurationCollection == null) return null;
		return Collections2.transform(splunkDestinationConfigurationCollection, new Function<SplunkDestinationConfiguration, software.amazon.awssdk.services.firehose.model.SplunkDestinationConfiguration>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.SplunkDestinationConfiguration apply(final SplunkDestinationConfiguration input) {
				return translateSplunkDestinationConfiguration(input);
			}
		});
	}

	 static software.amazon.awssdk.services.firehose.model.SplunkRetryOptions translateSplunkRetryOptions(final SplunkRetryOptions splunkRetryOptions) {
		if(splunkRetryOptions == null) return null;
		return software.amazon.awssdk.services.firehose.model.SplunkRetryOptions.builder()
				.durationInSeconds(splunkRetryOptions.getDurationInSeconds())
				.build();
	}

	 static Collection<software.amazon.awssdk.services.firehose.model.SplunkRetryOptions> translateSplunkRetryOptionsCollection(final Collection<SplunkRetryOptions> splunkRetryOptionsCollection) {
		if(splunkRetryOptionsCollection == null) return null;
		return Collections2.transform(splunkRetryOptionsCollection, new Function<SplunkRetryOptions, software.amazon.awssdk.services.firehose.model.SplunkRetryOptions>() {
			@Override
			public software.amazon.awssdk.services.firehose.model.SplunkRetryOptions apply(final SplunkRetryOptions input) {
				return translateSplunkRetryOptions(input);
			}
		});
	}

	static S3DestinationUpdate translateS3DestinationUpdate(final S3DestinationConfiguration s3DestinationConfiguration) {
		if(s3DestinationConfiguration == null) return null;
		return S3DestinationUpdate.builder()
				.roleARN(s3DestinationConfiguration.getRoleARN())
				.bucketARN(s3DestinationConfiguration.getBucketARN())
				.prefix(s3DestinationConfiguration.getPrefix())
				.bufferingHints(translateBufferingHints(s3DestinationConfiguration.getBufferingHints()))
				.compressionFormat(s3DestinationConfiguration.getCompressionFormat())
				.encryptionConfiguration(translateEncryptionConfiguration(s3DestinationConfiguration.getEncryptionConfiguration()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(s3DestinationConfiguration.getCloudWatchLoggingOptions()))
				.errorOutputPrefix(s3DestinationConfiguration.getErrorOutputPrefix())
				.build();
	}

	static ExtendedS3DestinationUpdate translateExtendedS3DestinationUpdate (final ExtendedS3DestinationConfiguration extendedS3DestinationUpdate) {
		if(extendedS3DestinationUpdate == null) return null;
		return ExtendedS3DestinationUpdate.builder()
				.bucketARN(extendedS3DestinationUpdate.getBucketARN())
				.bufferingHints(translateBufferingHints(extendedS3DestinationUpdate.getBufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(extendedS3DestinationUpdate.getCloudWatchLoggingOptions()))
				.compressionFormat(extendedS3DestinationUpdate.getCompressionFormat())
				.encryptionConfiguration(translateEncryptionConfiguration(extendedS3DestinationUpdate.getEncryptionConfiguration()))
				.prefix(extendedS3DestinationUpdate.getPrefix())
				.roleARN(extendedS3DestinationUpdate.getRoleARN())
				.processingConfiguration(translateProcessingConfiguration(extendedS3DestinationUpdate.getProcessingConfiguration()))
				.s3BackupMode(extendedS3DestinationUpdate.getS3BackupMode())
				.s3BackupUpdate(translateS3DestinationUpdate(extendedS3DestinationUpdate.getS3BackupConfiguration()))
				.errorOutputPrefix(extendedS3DestinationUpdate.getErrorOutputPrefix())
				.dataFormatConversionConfiguration(translateDataFormatConversionConfiguration(extendedS3DestinationUpdate.getDataFormatConversionConfiguration()))
				.build();
	}

	static RedshiftDestinationUpdate translateRedshiftDestinationUpdate(final RedshiftDestinationConfiguration redshiftDestinationConfiguration) {
		if(redshiftDestinationConfiguration == null) return null;
		return RedshiftDestinationUpdate.builder()
				.roleARN(redshiftDestinationConfiguration.getRoleARN())
				.clusterJDBCURL(redshiftDestinationConfiguration.getClusterJDBCURL())
				.username(redshiftDestinationConfiguration.getUsername())
				.password(redshiftDestinationConfiguration.getPassword())
				.s3Update(translateS3DestinationUpdate(redshiftDestinationConfiguration.getS3Configuration()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(redshiftDestinationConfiguration.getCloudWatchLoggingOptions()))
				.build();
	}

	static ElasticsearchDestinationUpdate translateElasticsearchDestinationUpdate(final ElasticsearchDestinationConfiguration elasticsearchDestinationConfiguration) {
		if(elasticsearchDestinationConfiguration == null) return null;
		return ElasticsearchDestinationUpdate.builder()
				.roleARN(elasticsearchDestinationConfiguration.getRoleARN())
				.domainARN(elasticsearchDestinationConfiguration.getDomainARN())
				.indexName(elasticsearchDestinationConfiguration.getIndexName())
				.clusterEndpoint(elasticsearchDestinationConfiguration.getClusterEndpoint())
				.indexRotationPeriod(elasticsearchDestinationConfiguration.getIndexRotationPeriod())
				.bufferingHints(translateElasticsearchBufferingHints(elasticsearchDestinationConfiguration.getBufferingHints()))
				.retryOptions(translateElasticsearchRetryOptions(elasticsearchDestinationConfiguration.getRetryOptions()))
				.s3Update(translateS3DestinationUpdate(elasticsearchDestinationConfiguration.getS3Configuration()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(elasticsearchDestinationConfiguration.getCloudWatchLoggingOptions()))
				.build();
	}

	static SplunkDestinationUpdate translateSplunkDestinationUpdate(final SplunkDestinationConfiguration splunkDestinationConfiguration) {
		if(splunkDestinationConfiguration == null) return null;
		return SplunkDestinationUpdate.builder()
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptions(splunkDestinationConfiguration.getCloudWatchLoggingOptions()))
				.hecAcknowledgmentTimeoutInSeconds(splunkDestinationConfiguration.getHECAcknowledgmentTimeoutInSeconds())
				.hecEndpoint(splunkDestinationConfiguration.getHECEndpoint())
				.hecEndpointType(splunkDestinationConfiguration.getHECEndpointType())
				.hecToken(splunkDestinationConfiguration.getHECToken())
				.processingConfiguration(translateProcessingConfiguration(splunkDestinationConfiguration.getProcessingConfiguration()))
				.retryOptions(translateSplunkRetryOptions(splunkDestinationConfiguration.getRetryOptions()))
				.s3BackupMode(splunkDestinationConfiguration.getS3BackupMode())
				.s3Update(translateS3DestinationUpdate(splunkDestinationConfiguration.getS3Configuration()))
				.build();
	}

	static S3DestinationConfiguration translateS3DestinationConfigurationToCfnModel(final S3DestinationDescription s3DestinationDescription) {
		return s3DestinationDescription == null ? null : S3DestinationConfiguration.builder()
				.bucketARN(s3DestinationDescription.bucketARN())
				.bufferingHints(translateBufferingHintsToCfnModel(s3DestinationDescription.bufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptionsToCfnModel(s3DestinationDescription.cloudWatchLoggingOptions()))
				.compressionFormat(s3DestinationDescription.compressionFormatAsString())
				.encryptionConfiguration(translateEncryptionConfigurationToCfnModel(s3DestinationDescription.encryptionConfiguration()))
				.errorOutputPrefix(s3DestinationDescription.errorOutputPrefix())
				.prefix(s3DestinationDescription.prefix())
				.roleARN(s3DestinationDescription.roleARN())
				.build();
	}

	static ExtendedS3DestinationConfiguration translateExtendedS3DestinationConfigurationToCfnModel(
			final ExtendedS3DestinationDescription extendedS3DestinationDescription) {
		return extendedS3DestinationDescription == null ? null : ExtendedS3DestinationConfiguration.builder()
				.bucketARN(extendedS3DestinationDescription.bucketARN())
				.bufferingHints(translateBufferingHintsToCfnModel(extendedS3DestinationDescription.bufferingHints()))
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptionsToCfnModel(extendedS3DestinationDescription.cloudWatchLoggingOptions()))
				.compressionFormat(extendedS3DestinationDescription.compressionFormatAsString())
				.dataFormatConversionConfiguration(translateDataFormatConversionConfigurationToCfnModel(extendedS3DestinationDescription.dataFormatConversionConfiguration()))
				.encryptionConfiguration(translateEncryptionConfigurationToCfnModel(extendedS3DestinationDescription.encryptionConfiguration()))
				.errorOutputPrefix(extendedS3DestinationDescription.errorOutputPrefix())
				.prefix(extendedS3DestinationDescription.prefix())
				.processingConfiguration(translateProcessingConfigurationToCfnModel(extendedS3DestinationDescription.processingConfiguration()))
				.roleARN(extendedS3DestinationDescription.roleARN())
				.s3BackupConfiguration(translateS3DestinationConfigurationToCfnModel(extendedS3DestinationDescription.s3BackupDescription()))
				.s3BackupMode(extendedS3DestinationDescription.s3BackupModeAsString())
				.build();
	}

	static RedshiftDestinationConfiguration translateRedshiftDestinationToCfnModel(final RedshiftDestinationDescription redshiftDestinationDescription) {
		return redshiftDestinationDescription == null ? null : RedshiftDestinationConfiguration.builder()
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptionsToCfnModel(redshiftDestinationDescription.cloudWatchLoggingOptions()))
				.clusterJDBCURL(redshiftDestinationDescription.clusterJDBCURL())
				.copyCommand(CopyCommand.builder()
						.copyOptions(redshiftDestinationDescription.copyCommand().copyOptions())
						.dataTableColumns(redshiftDestinationDescription.copyCommand().dataTableColumns())
						.dataTableName(redshiftDestinationDescription.copyCommand().dataTableName())
						.build())
				.s3Configuration(translateS3DestinationConfigurationToCfnModel(redshiftDestinationDescription.s3DestinationDescription()))
				.processingConfiguration(translateProcessingConfigurationToCfnModel(redshiftDestinationDescription.processingConfiguration()))
				.roleARN(redshiftDestinationDescription.roleARN())
				.username(redshiftDestinationDescription.username())
				.build();
	}

	static ElasticsearchDestinationConfiguration translateElasticsearchDestinationConfigurationToCfnModel(
			final ElasticsearchDestinationDescription elasticsearchDestinationDescription) {
		return elasticsearchDestinationDescription == null ? null : ElasticsearchDestinationConfiguration.builder()
				.bufferingHints(elasticsearchDestinationDescription.bufferingHints() == null ? null : ElasticsearchBufferingHints.builder()
						.intervalInSeconds(elasticsearchDestinationDescription.bufferingHints().intervalInSeconds())
						.sizeInMBs(elasticsearchDestinationDescription.bufferingHints().sizeInMBs())
						.build())
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptionsToCfnModel(elasticsearchDestinationDescription.cloudWatchLoggingOptions()))
				.clusterEndpoint(elasticsearchDestinationDescription.clusterEndpoint())
				.domainARN(elasticsearchDestinationDescription.domainARN())
				.indexName(elasticsearchDestinationDescription.indexName())
				.indexRotationPeriod(elasticsearchDestinationDescription.indexRotationPeriodAsString())
				.processingConfiguration(translateProcessingConfigurationToCfnModel(elasticsearchDestinationDescription.processingConfiguration()))
				.retryOptions(elasticsearchDestinationDescription.retryOptions() == null ? null : ElasticsearchRetryOptions.builder()
						.durationInSeconds(elasticsearchDestinationDescription.retryOptions().durationInSeconds())
						.build())
				.roleARN(elasticsearchDestinationDescription.roleARN())
				.s3BackupMode(elasticsearchDestinationDescription.s3BackupModeAsString())
				.s3Configuration(translateS3DestinationConfigurationToCfnModel(elasticsearchDestinationDescription.s3DestinationDescription()))
				.typeName(elasticsearchDestinationDescription.typeName())
				.build();
	}

	static SplunkDestinationConfiguration translateSplunkDestinationConfigurationToCfnModel(final SplunkDestinationDescription splunkDestinationDescription) {
		return splunkDestinationDescription == null ? null : SplunkDestinationConfiguration.builder()
				.cloudWatchLoggingOptions(translateCloudWatchLoggingOptionsToCfnModel(splunkDestinationDescription.cloudWatchLoggingOptions()))
				.hECAcknowledgmentTimeoutInSeconds(splunkDestinationDescription.hecAcknowledgmentTimeoutInSeconds())
				.hECEndpoint(splunkDestinationDescription.hecEndpoint())
				.hECEndpointType(splunkDestinationDescription.hecEndpointTypeAsString())
				.hECToken(splunkDestinationDescription.hecToken())
				.processingConfiguration(translateProcessingConfigurationToCfnModel(splunkDestinationDescription.processingConfiguration()))
				.retryOptions(splunkDestinationDescription.retryOptions() == null ? null : SplunkRetryOptions.builder()
						.durationInSeconds(splunkDestinationDescription.retryOptions().durationInSeconds())
						.build())
				.s3BackupMode(splunkDestinationDescription.s3BackupModeAsString())
				.s3Configuration(translateS3DestinationConfigurationToCfnModel(splunkDestinationDescription.s3DestinationDescription()))
				.build();
	}

	static DataFormatConversionConfiguration translateDataFormatConversionConfigurationToCfnModel(
			final software.amazon.awssdk.services.firehose.model.DataFormatConversionConfiguration dataFormatConversionConfiguration) {
		if (dataFormatConversionConfiguration == null) {
			return null;
		}
		return DataFormatConversionConfiguration.builder()
				.enabled(dataFormatConversionConfiguration.enabled())
				.inputFormatConfiguration(translateInputFormatConfigurationToCfnModel(dataFormatConversionConfiguration.inputFormatConfiguration()))
				.outputFormatConfiguration(translateOutputFormatConfigurationToCfnModel(dataFormatConversionConfiguration.outputFormatConfiguration()))
				.schemaConfiguration(translateSchemaConfigurationToCfnModel(dataFormatConversionConfiguration.schemaConfiguration()))
				.build();
	}

	static SchemaConfiguration translateSchemaConfigurationToCfnModel(
			software.amazon.awssdk.services.firehose.model.SchemaConfiguration schemaConfiguration) {
		return schemaConfiguration == null ? null : SchemaConfiguration.builder()
				.catalogId(schemaConfiguration.catalogId())
				.databaseName(schemaConfiguration.databaseName())
				.region(schemaConfiguration.region())
				.roleARN(schemaConfiguration.roleARN())
				.tableName(schemaConfiguration.tableName())
				.versionId(schemaConfiguration.versionId())
				.build();
	}

	static OutputFormatConfiguration translateOutputFormatConfigurationToCfnModel(
			software.amazon.awssdk.services.firehose.model.OutputFormatConfiguration outputFormatConfiguration) {
		if (outputFormatConfiguration == null) {
			return null;
		}
		val serializer = outputFormatConfiguration.serializer();
		return OutputFormatConfiguration.builder()
				.serializer(Serializer.builder()
						.parquetSerDe(serializer.parquetSerDe() == null ? null
								: ParquetSerDe.builder()
								        .blockSizeBytes(serializer.parquetSerDe().blockSizeBytes())
								        .compression(serializer.parquetSerDe().compressionAsString())
								        .enableDictionaryCompression(serializer.parquetSerDe().enableDictionaryCompression())
								        .maxPaddingBytes(serializer.parquetSerDe().maxPaddingBytes())
								        .pageSizeBytes(serializer.parquetSerDe().pageSizeBytes())
								        .writerVersion(serializer.parquetSerDe().writerVersionAsString())
								        .build())
						.orcSerDe(serializer.orcSerDe() == null ? null
								: OrcSerDe.builder()
								        .blockSizeBytes(serializer.orcSerDe().blockSizeBytes())
								        .bloomFilterColumns(serializer.orcSerDe().bloomFilterColumns())
								        .bloomFilterFalsePositiveProbability(serializer.orcSerDe().bloomFilterFalsePositiveProbability())
								        .compression(serializer.orcSerDe().compressionAsString())
								        .dictionaryKeyThreshold(serializer.orcSerDe().dictionaryKeyThreshold())
								        .enablePadding(serializer.orcSerDe().enablePadding())
								        .formatVersion(serializer.orcSerDe().formatVersionAsString())
								        .paddingTolerance(serializer.orcSerDe().paddingTolerance())
								        .rowIndexStride(serializer.orcSerDe().rowIndexStride())
								        .stripeSizeBytes(serializer.orcSerDe().stripeSizeBytes())
								        .build())
						.build())
				.build();
	}

	static InputFormatConfiguration translateInputFormatConfigurationToCfnModel(
			software.amazon.awssdk.services.firehose.model.InputFormatConfiguration inputFormatConfiguration) {
		if (inputFormatConfiguration == null) {
			return null;
		}
		val deserializer = inputFormatConfiguration.deserializer();
		return InputFormatConfiguration.builder()
				.deserializer(Deserializer.builder().
						openXJsonSerDe(deserializer.openXJsonSerDe() == null ? null
								:OpenXJsonSerDe.builder()
								.caseInsensitive(deserializer.openXJsonSerDe().caseInsensitive())
								.columnToJsonKeyMappings(deserializer.openXJsonSerDe().columnToJsonKeyMappings())
								.convertDotsInJsonKeysToUnderscores(deserializer.openXJsonSerDe().convertDotsInJsonKeysToUnderscores()).build())
						.hiveJsonSerDe(deserializer.hiveJsonSerDe() == null ? null
								: HiveJsonSerDe.builder().timestampFormats(deserializer.hiveJsonSerDe().timestampFormats()).build())
						.build())
				.build();
	}

	static ProcessingConfiguration translateProcessingConfigurationToCfnModel(
			final software.amazon.awssdk.services.firehose.model.ProcessingConfiguration processingConfiguration) {
		return processingConfiguration == null ? null : ProcessingConfiguration.builder()
				.enabled(processingConfiguration.enabled())
				.processors(processingConfiguration.hasProcessors()
						? processingConfiguration.processors()
						    .stream().map(processor -> Processor.builder()
								.parameters(processor.hasParameters()
										? processor.parameters().stream().map(p -> ProcessorParameter.builder()
										    .parameterName(p.parameterNameAsString())
										    .parameterValue(p.parameterValue())
										    .build()).collect(Collectors.toList())
										: null)
								.type(processor.typeAsString())
								.build())
						    .collect(Collectors.toList())
						: null)
				.build();
	}

	static EncryptionConfiguration translateEncryptionConfigurationToCfnModel(
			final software.amazon.awssdk.services.firehose.model.EncryptionConfiguration encryptionConfiguration) {
		return encryptionConfiguration == null ? null : EncryptionConfiguration.builder()
				.kMSEncryptionConfig(encryptionConfiguration.kmsEncryptionConfig() != null ? KMSEncryptionConfig.builder()
						.aWSKMSKeyARN(encryptionConfiguration.kmsEncryptionConfig().awskmsKeyARN()).build()  : null)
				.noEncryptionConfig(encryptionConfiguration.noEncryptionConfigAsString())
				.build();
	}

	static CloudWatchLoggingOptions translateCloudWatchLoggingOptionsToCfnModel(
			final software.amazon.awssdk.services.firehose.model.CloudWatchLoggingOptions cloudWatchLoggingOptions) {
		return cloudWatchLoggingOptions == null ? null : CloudWatchLoggingOptions.builder()
				.enabled(cloudWatchLoggingOptions.enabled())
				.logGroupName(cloudWatchLoggingOptions.logGroupName())
				.logStreamName(cloudWatchLoggingOptions.logStreamName())
				.build();
	}

	static BufferingHints translateBufferingHintsToCfnModel(final software.amazon.awssdk.services.firehose.model.BufferingHints bufferingHints) {
		return bufferingHints == null ? null : BufferingHints.builder()
				.intervalInSeconds(bufferingHints.intervalInSeconds())
				.sizeInMBs(bufferingHints.sizeInMBs())
				.build();
	}

	static KinesisStreamSourceConfiguration translateKinesisStreamSourceConfigurationToCfnModel(
			final SourceDescription sourceDescription) {
		if (sourceDescription == null) {
			return null;
		}
		val kinesisStreamSourceDescription = sourceDescription.kinesisStreamSourceDescription();
		return kinesisStreamSourceDescription == null ? null : KinesisStreamSourceConfiguration.builder()
				.kinesisStreamARN(kinesisStreamSourceDescription.kinesisStreamARN())
				.roleARN(kinesisStreamSourceDescription.roleARN())
				.build();
	}
}
