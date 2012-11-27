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

/**
 * Refeeds all the documents which matches the type and the (optional) query.
 *
 * @author Peter Karich
 */
public class ReIndexAction extends BaseRestHandler {

    @Inject public ReIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a reindex
        controller.registerHandler(PUT, "/{index}/{type}/_reindex", this);
        controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexAction.handleRequest [{}]", request.toString());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String oldIndexName = request.param("index");
            String newIndexName = request.param("newIndex");
            if (newIndexName == null || newIndexName.isEmpty())
                newIndexName = oldIndexName;

            String oldType = request.param("type");
            String newType = request.param("newType");
            if (newType == null || newType.isEmpty())
                newType = oldType;

            boolean withVersion = request.paramAsBoolean("withVersion", false);
            int keepTimeInMinutes = request.paramAsInt("keepTimeInMinutes", 100);
            int hitsPerPage = request.paramAsInt("hitsPerPage", 100);
            // TODO use the query as filter!
            String query = request.contentAsString();
            boolean ownCluster = request.hasParam("searchHost");
            MySearchResponse rsp;
            if (ownCluster) {
                SearchRequestBuilder srb = createSearch(oldIndexName, oldType, query,
                        hitsPerPage, withVersion, keepTimeInMinutes);
                SearchResponse sr = srb.execute().actionGet();
                rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);
            } else {
                int port = request.paramAsInt("searchPort", 9200);
                String host = request.param("searchHost", "localhost");
                // TODO cluster
                rsp = new MySearchResponseJson(host, port, oldIndexName, oldType, query,
                        hitsPerPage, withVersion, keepTimeInMinutes);
            }

            reindex(rsp, newIndexName, newType, withVersion);
            logger.info("Finished copying of index " + oldIndexName + " into " + newIndexName + ", query " + query);
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
        if (query == null || query.trim().isEmpty())
            query = "{ \"match_all\": {} }";
        return client.prepareSearch(oldIndexName).
                setTypes(oldType).
                setVersion(withVersion).
                setQuery(query).
                setSize(hitsPerPage).
                setSearchType(SearchType.SCAN).
                setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes));
    }

    public int reindex(MySearchResponse rsp, String newIndex, String newType, boolean withVersion) {
        boolean flushEnabled = false;
        long total = rsp.hits().totalHits();
        int collectedResults = 0;
        int failed = 0;
        while (true) {
            StopWatch queryWatch = new StopWatch().start();
            int currentResults = rsp.doScoll();
            if (currentResults == 0)
                break;

            queryWatch.stop();
            StopWatch updateWatch = new StopWatch().start();
            failed += bulkUpdate(rsp.hits(), newIndex, newType, withVersion).size();
            if (flushEnabled)
                client.admin().indices().flush(new FlushRequest(newIndex)).actionGet();

            updateWatch.stop();
            collectedResults += currentResults;
            logger.info("Progress " + collectedResults + "/" + total
                    + " update:" + updateWatch.totalTime().getSeconds() + " query:"
                    + queryWatch.totalTime().getSeconds() + " failed:" + failed);
        }
        String str = "found " + total + ", collected:" + collectedResults;
        if (failed > 0)
            logger.warn(failed + " FAILED documents! " + str);
        else
            logger.info(str);
        return collectedResults;
    }

    Collection<Integer> bulkUpdate(MySearchHits objects, String indexName,
            String newType, boolean withVersion) {
        BulkRequestBuilder brb = client.prepareBulk();
        for (MySearchHit hit : objects.getHits()) {            
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
