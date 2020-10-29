package com.amazonaws.kinesisfirehose.deliverystream;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.firehose.model.*;
import software.amazon.cloudformation.proxy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.stream.Collectors;

import static com.amazonaws.kinesisfirehose.deliverystream.ListHandler.LIST_RESULT_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import lombok.val;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    private ListHandler listHandler;

    @BeforeEach
    public void setup() {
        listHandler = new ListHandler();
    }

    @Test
    public void testListWithoutNextToken() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .build();

        final ListDeliveryStreamsResponse listResponse = ListDeliveryStreamsResponse.builder()
                .deliveryStreamNames(Collections.emptyList())
                .hasMoreDeliveryStreams(false)
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(ListDeliveryStreamsRequest.class), any()))
                .thenReturn(listResponse);
        val response = listHandler.handleRequest(
                proxy, request, null, logger);
        assertThat(response.getResourceModels().isEmpty()).isTrue();
        assertThat(response.getNextToken()).isNull();


        // Test another branch where you got set of results in the ListDeliveryStreamResponse, but Next token is null.
        final ListDeliveryStreamsResponse listResponse2 = ListDeliveryStreamsResponse.builder()
            .deliveryStreamNames(ImmutableList.of("deliveryStreamName1"))
            .hasMoreDeliveryStreams(false)
            .build();

        when(proxy.injectCredentialsAndInvokeV2(any(ListDeliveryStreamsRequest.class), any()))
            .thenReturn(listResponse2);
        val response2 = listHandler.handleRequest(
            proxy, request, null, logger);
        assertThat(response2.getResourceModels().isEmpty()).isFalse();
        assertThat(response2.getNextToken()).isNull();
    }

    @Test
    public void testListWithNextToken() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .nextToken("test-delivery-stream-0")
                .build();

        val listRequest = ListDeliveryStreamsRequest.builder()
                .limit(LIST_RESULT_LIMIT)
                .exclusiveStartDeliveryStreamName("test-delivery-stream-0")
                .build();

        val responseModels = ImmutableList.of("test-delivery-stream-1", "test-delivery-stream-2");
        final ListDeliveryStreamsResponse listResponse = ListDeliveryStreamsResponse.builder()
                .deliveryStreamNames(responseModels)
                .hasMoreDeliveryStreams(true)
                .build();

        when(proxy.injectCredentialsAndInvokeV2(eq(listRequest), any()))
                .thenReturn(listResponse);
        val response = listHandler.handleRequest(
                proxy, request, null, logger);
        assertThat(response.getResourceModels().stream().map(m -> m.getDeliveryStreamName()).collect(Collectors.toList()))
                .isEqualTo(responseModels);
        assertThat(response.getNextToken()).isEqualTo("test-delivery-stream-2");
    }

    @Test
    public void testListWithException() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .nextToken("test-delivery-stream-0")
                .build();

        when(proxy.injectCredentialsAndInvokeV2(any(ListDeliveryStreamsRequest.class), any()))
                .thenThrow(FirehoseException.builder().message("test").build());
        val response = listHandler.handleRequest(
                proxy, request, null, logger);
        assertThat(response.getStatus())
                .isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.ServiceInternalError);
    }
}
