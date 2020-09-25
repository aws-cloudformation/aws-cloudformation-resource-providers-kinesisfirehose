package com.amazonaws.kinesisfirehose.deliverystream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.val;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DeleteDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeleteDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamEncryptionConfigurationInput;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsRequest;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsResponse;
import software.amazon.awssdk.services.firehose.model.ListTagsForDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StartDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionRequest;
import software.amazon.awssdk.services.firehose.model.StopDeliveryStreamEncryptionResponse;
import software.amazon.awssdk.services.firehose.model.Tag;
import software.amazon.awssdk.services.firehose.model.TagDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.TagDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.UntagDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.UntagDeliveryStreamResponse;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationRequest;
import software.amazon.awssdk.services.firehose.model.UpdateDestinationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;

@Builder
public class FirehoseAPIWrapper {

    @NonNull
    private AmazonWebServicesClientProxy clientProxy;
    @NonNull
    private FirehoseClient firehoseClient;

    public List<Tag> listAllTagsOnDeliveryStream(final String deliveryStreamName, final int resultLimit) {
        String startTagKey = null;
        Boolean hasMoreTags = false;
        List<Tag> tags = new ArrayList<>();
        do {
            val req = ListTagsForDeliveryStreamRequest.builder()
                .deliveryStreamName(deliveryStreamName)
                .exclusiveStartTagKey(startTagKey)
                .limit(resultLimit)
                .build();
            val resp = clientProxy.injectCredentialsAndInvokeV2(req,
                firehoseClient::listTagsForDeliveryStream);
            if (resp.tags() != null && resp.tags().size() > 0) {
                tags.addAll(resp.tags());
                startTagKey = tags.get(tags.size() - 1).key();
                hasMoreTags = resp.hasMoreTags();
            }
        } while (hasMoreTags != null && hasMoreTags);
        return tags;
    }

    public TagDeliveryStreamResponse tagDeliveryStream(final String deliveryStreamName,
        Collection<Tag> tags) {
        val req = TagDeliveryStreamRequest.builder()
            .deliveryStreamName(deliveryStreamName)
            .tags(tags)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(req,
            firehoseClient::tagDeliveryStream);
    }

    public UntagDeliveryStreamResponse untagDeliveryStream(final String deliveryStreamName,
        final Collection<String> tagKeys) {
        val req = UntagDeliveryStreamRequest.builder()
            .deliveryStreamName(deliveryStreamName)
            .tagKeys(tagKeys)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(req,
            firehoseClient::untagDeliveryStream);
    }

    public DescribeDeliveryStreamResponse describeDeliveryStream(final String deliveryStreamName) {
        return clientProxy.injectCredentialsAndInvokeV2(DescribeDeliveryStreamRequest.builder()
                .deliveryStreamName(deliveryStreamName)
                .build(),
            firehoseClient::describeDeliveryStream);
    }

    public CreateDeliveryStreamResponse createDeliveryStream(
        final CreateDeliveryStreamRequest createDeliveryStreamRequest) {
        return clientProxy.injectCredentialsAndInvokeV2(createDeliveryStreamRequest,
            firehoseClient::createDeliveryStream);
    }

    public DeleteDeliveryStreamResponse deleteDeliveryStream(final String deliveryStreamName,
        final boolean allowForceDelete) {
        val deleteDeliveryStreamRequest = DeleteDeliveryStreamRequest.builder()
            .deliveryStreamName(deliveryStreamName)
            .allowForceDelete(allowForceDelete)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(deleteDeliveryStreamRequest,
            firehoseClient::deleteDeliveryStream);
    }

    public StartDeliveryStreamEncryptionResponse startDeliveryStreamEncryption(
        final String deliveryStreamName,
        final DeliveryStreamEncryptionConfigurationInput deliveryStreamEncryptionConfigurationInput) {
        val startEncryptionRequest = StartDeliveryStreamEncryptionRequest.builder()
            .deliveryStreamName(deliveryStreamName)
            .deliveryStreamEncryptionConfigurationInput(deliveryStreamEncryptionConfigurationInput)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(startEncryptionRequest,
            firehoseClient::startDeliveryStreamEncryption);
    }

    public StopDeliveryStreamEncryptionResponse stopDeliveryStreamEncryption(
        final String deliveryStreamName) {
        val stopEncryptionRequest = StopDeliveryStreamEncryptionRequest.builder()
            .deliveryStreamName(deliveryStreamName)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(stopEncryptionRequest,
            firehoseClient::stopDeliveryStreamEncryption);
    }

    public UpdateDestinationResponse updateDestination(
        final UpdateDestinationRequest updateDestinationRequest) {
        return clientProxy.injectCredentialsAndInvokeV2(updateDestinationRequest,
            firehoseClient::updateDestination);
    }

    public ListDeliveryStreamsResponse listDeliveryStreams(String startDeliveryStreamName, int resultLimit){
        val req = ListDeliveryStreamsRequest.builder()
            .limit(resultLimit)
            .exclusiveStartDeliveryStreamName(startDeliveryStreamName)
            .build();
        return clientProxy.injectCredentialsAndInvokeV2(req, firehoseClient::listDeliveryStreams);
    }

}
