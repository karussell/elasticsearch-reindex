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

        if (controller != null) {
            // Define REST endpoints to do a reindex
            controller.registerHandler(PUT, "/{index}/{type}/_reindex", this);
            controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
        }
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexAction.handleRequest [{}]", request.params());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String newIndexName = request.param("index");
            String searchIndexName = request.param("searchIndex");
            if (searchIndexName == null || searchIndexName.isEmpty())
                searchIndexName = newIndexName;

            String newType = request.param("type");
            String searchType = request.param("searchType");
            if (searchType == null || searchType.isEmpty())
                searchType = newType;

            int searchPort = request.paramAsInt("searchPort", 9200);
            String searchHost = request.param("searchHost", "localhost");
            boolean localAction = "localhost".equals(searchHost) && searchPort == 9200;
            boolean withVersion = request.paramAsBoolean("withVersion", false);
            int keepTimeInMinutes = request.paramAsInt("keepTimeInMinutes", 30);
            int hitsPerPage = request.paramAsInt("hitsPerPage", 1000);
            float waitInSeconds = request.paramAsFloat("waitInSeconds", 0);
            String basicAuthCredentials = request.param("credentials", "");
            String filter = request.content().toUtf8();
            MySearchResponse rsp;
            if (localAction) {
                SearchRequestBuilder srb = createScrollSearch(searchIndexName, searchType, filter,
                        hitsPerPage, withVersion, keepTimeInMinutes);
                SearchResponse sr = srb.execute().actionGet();
                rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);
            } else {
                // TODO make it possible to restrict to a cluster
                rsp = new MySearchResponseJson(searchHost, searchPort, searchIndexName, searchType, filter,
                        basicAuthCredentials, hitsPerPage, withVersion, keepTimeInMinutes);
            }

            // TODO make async and allow control of process from external (e.g. stopping etc)
            // or just move stuff into a river?
            reindex(rsp, newIndexName, newType, withVersion, waitInSeconds);

            // TODO reindex again all new items => therefor we need a timestamp field to filter
            // + how to combine with existing filter?

            logger.info("Finished reindexing of index " + searchIndexName + " into " + newIndexName + ", query " + filter);
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }

    SearchRequestBuilder createScrollSearch(String oldIndexName, String oldType, String filter,
            int hitsPerPage, boolean withVersion, int keepTimeInMinutes) {
        SearchRequestBuilder srb = client.prepareSearch(oldIndexName).
                setTypes(oldType).
                setVersion(withVersion).
                setSize(hitsPerPage).
                setSearchType(SearchType.SCAN).
                setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes));

        if (filter != null && !filter.trim().isEmpty())
            srb.setFilter(filter);
        return srb;
    }

    public int reindex(MySearchResponse rsp, String newIndex, String newType, boolean withVersion,
            float waitSeconds) {
        boolean flushEnabled = false;
        long total = rsp.hits().totalHits();
        int collectedResults = 0;
        int failed = 0;
        while (true) {
            if (collectedResults > 0 && waitSeconds > 0) {
                try {
                    Thread.sleep(Math.round(waitSeconds * 1000));
                } catch (InterruptedException ex) {
                    break;
                }
            }
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
            logger.debug("Progress " + collectedResults + "/" + total
                    + ". Time of update:" + updateWatch.totalTime().getSeconds() + " query:"
                    + queryWatch.totalTime().getSeconds() + " failed:" + failed);
        }
        String str = "found " + total + ", collected:" + collectedResults
                + ", transfered:" + (float) rsp.bytes() / (1 << 20) + "MB";
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
