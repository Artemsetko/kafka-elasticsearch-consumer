package org.elasticsearch.kafka.indexer.service;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.kafka.indexer.exception.IndexerESNotRecoverableException;
import org.elasticsearch.kafka.indexer.exception.IndexerESRecoverableException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class ElasticSearchBatchServiceTest {

    @Mock
    private ElasticSearchClientService elasticSearchClientService;

    private ElasticSearchBatchService elasticSearchBatchService = new ElasticSearchBatchService();

    private BulkRequestBuilder mockedBulkRequestBuilder = Mockito.mock(BulkRequestBuilder.class);
    private IndexRequestBuilder mockedIndexRequestBuilder = Mockito.mock(IndexRequestBuilder.class);
    private IndexRequest mockedIndexRequest = Mockito.mock(IndexRequest.class);
    private RestHighLevelClient mockedRestHighLevelClient = Mockito.mock(RestHighLevelClient.class);;
    private ListenableActionFuture<BulkResponse> mockedActionFuture = Mockito.mock(ListenableActionFuture.class);
    private BulkResponse mockedBulkResponse = Mockito.mock(BulkResponse.class);
    private String testIndexName = "unitTestsIndex";
    private String testIndexType = "unitTestsType";

    /**
     * @throws java.lang.Exception
     */

    @Before
    public void setUp() throws Exception {
        // Mock all required ES classes and methods
        elasticSearchBatchService.setElasticSearchClientService(elasticSearchClientService);
        //	Mockito.when(elasticSearchClientService.prepareBulk()).thenReturn(mockedBulkRequestBuilder);
        Mockito.when(mockedIndexRequestBuilder.setSource(Matchers.anyString())).thenReturn(mockedIndexRequestBuilder);
        Mockito.when(mockedBulkRequestBuilder.execute()).thenReturn(mockedActionFuture);
        Mockito.when(mockedActionFuture.actionGet()).thenReturn(mockedBulkResponse);
        // mock the number of messages in the bulk index request to be 1
        Mockito.when(mockedBulkRequestBuilder.numberOfActions()).thenReturn(1);

        // Mockito.when(elasticIndexHandler.getIndexName(null)).thenReturn(testIndexName);
        // Mockito.when(elasticIndexHandler.getIndexType(null)).thenReturn(testIndexType);
    }

    @Test
    public void testAddEventToBulkRequest_withUUID_withRouting() {
        String message = "test message";
        String eventUUID = "eventUUID";
        // boolean needsRouting = true;
		Mockito.when(elasticSearchClientService.prepareIndexRequest(message, testIndexName, testIndexType, eventUUID)).thenReturn(new IndexRequest().id(eventUUID).type(testIndexType).source(message, XContentType.JSON).index(testIndexName));

		try {
            elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
                    eventUUID);
        } catch (Exception e) {
            fail("Unexpected exception from unit test: " + e.getMessage());
        }

		Assert.assertEquals(elasticSearchBatchService.getBulkRequest().numberOfActions(),1);
        Mockito.verify(elasticSearchClientService, times(1)).prepareIndexRequest(message, testIndexName, testIndexType, eventUUID);
        // verify that routing is set to use eventUUID

    }

    @Test
    public void testAddEventToBulkRequest_withIndexRequest_withRouting() {
        String message = "test message";
        String eventUUID = "eventUUID";
        boolean needsRouting = true;

        Mockito.when(elasticSearchClientService.prepareIndexRequest(message, testIndexName, testIndexType, eventUUID)).thenReturn(mockedIndexRequest);

        try {
            elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
                    String.valueOf(needsRouting));
        } catch (Exception e) {
            fail("Unexpected exception from unit test: " + e.getMessage());
        }

        Mockito.verify(mockedIndexRequest).routing(String.valueOf(needsRouting));
        Assert.assertEquals(elasticSearchBatchService.getBulkRequest().numberOfActions(),1);
        Mockito.verify(elasticSearchClientService, times(1)).prepareIndexRequest(message, testIndexName, testIndexType, eventUUID);

    }

    @Test
    public void testPostToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException, ExecutionException, IOException {
        String message = "test message";
        String eventUUID = "eventUUID";

        Mockito.when(elasticSearchClientService.getEsClient()).thenReturn(mockedRestHighLevelClient);
        Mockito.when(mockedRestHighLevelClient.bulk(any(), any(RequestOptions.class))).thenReturn(mockedBulkResponse);
        Mockito.when(elasticSearchClientService.prepareIndexRequest(message, testIndexName, testIndexType, eventUUID)).thenReturn(new IndexRequest().id(eventUUID).type(testIndexType).source(message, XContentType.JSON).index(testIndexName));
        elasticSearchBatchService.addEventToBulkRequest(message, testIndexName, testIndexType, eventUUID,
                eventUUID);
        elasticSearchBatchService.postToElasticSearch();
       // Mockito.verify(elasticSearchClientService).getEsClient();

    }


}
