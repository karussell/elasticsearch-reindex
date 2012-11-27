package com.pannous.es.reindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * @see issue 1500 https://github.com/elasticsearch/elasticsearch/issues/1500
 *
 * Only indices with the rolling alias are involved into rolling.
 * @author Peter Karich
 */
public class ReIndexAction extends BaseRestHandler {

    @Inject public ReIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a roll further and to change the create-index-settings!        
        controller.registerHandler(PUT, "/{index}/{type}/_reindex", this);
        controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
        logger.info("ReIndexAction constructor [{}]", settings.toString());
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexAction.handleRequest [{}]", request.toString());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String oldIndexName = request.param("index");
            String newIndexName = request.param("newIndex");
            if (newIndexName.isEmpty())
                newIndexName = oldIndexName;

            String oldType = request.param("type");
            String newType = request.param("newType");

            boolean withVersion = false;
            int keepTimeInMinutes = 100;
            int hitsPerPage = 100;
            SearchRequestBuilder srb = createSearch(oldIndexName, oldType, request.contentAsString(),
                    hitsPerPage, withVersion, keepTimeInMinutes);
            int collectedResults = reindex(srb, newIndexName, newType, keepTimeInMinutes, withVersion);
            logger.info("Finished copying of index:" + newIndexName + ", collected results:" + collectedResults);
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }

    SearchRequestBuilder createSearch(String oldIndexName, String oldType, String query,
            int hitsPerPage, boolean withVersion, int keepTimeInMinutes) {
        return client.prepareSearch(oldIndexName).
                setTypes(oldType).
                setVersion(withVersion).
                setQuery(query).
                setSize(hitsPerPage).
                setSearchType(SearchType.SCAN).
                setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes));
    }

    public int reindex(SearchRequestBuilder srb, String newIndex, String newType,
            int keepTimeInMinutes, boolean withVersion) {

        boolean flushEnabled = false;
        SearchResponse rsp = srb.execute().actionGet();
        long total = rsp.hits().totalHits();
        int collectedResults = 0;
        while (true) {
            StopWatch queryWatch = new StopWatch().start();
            rsp = client.prepareSearchScroll(rsp.scrollId()).
                    setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes)).execute().actionGet();
            long currentResults = rsp.hits().hits().length;
            if (currentResults == 0)
                break;

            queryWatch.stop();
            StopWatch updateWatch = new StopWatch().start();
            int failed = bulkUpdate(rsp.getHits(), newIndex, newType, withVersion).size();
            if (flushEnabled)
                client.admin().indices().flush(new FlushRequest(newIndex)).actionGet();

            updateWatch.stop();
            collectedResults += currentResults;
            logger.info("Progress " + collectedResults + "/" + total
                    + " update:" + updateWatch.totalTime().getSeconds() + " query:" + queryWatch.totalTime().getSeconds() + " failed:" + failed);
        }
        return collectedResults;
    }

    public Collection<Integer> bulkUpdate(SearchHits objects, String indexName,
            String newType, boolean withVersion) {
        BulkRequestBuilder brb = client.prepareBulk();

        for (SearchHit hit : objects.getHits()) {
            if (hit.id() == null || hit.id().isEmpty()) {
                logger.warn("Skipped object without id when bulkUpdate:" + hit);
                continue;
            }

            try {
                IndexRequest indexReq = Requests.indexRequest(indexName).type(newType).id(hit.id()).source(hit.source());
                if (withVersion)
                    indexReq.version(hit.version());

                brb.add(indexReq);
            } catch (Exception ex) {
                logger.warn("Cannot add object:" + hit + " to bulkIndexing action." + ex.getMessage());
            }
        }
        if (brb.numberOfActions() > 0) {
            BulkResponse rsp = brb.execute().actionGet();
            if (rsp.hasFailures()) {
                List<Integer> list = new ArrayList<Integer>(rsp.items().length);
                for (BulkItemResponse br : rsp.items()) {
                    if (br.isFailed())
                        list.add(br.itemId());
                }
                return list;
            }

        }

        return Collections.emptyList();
    }
}
