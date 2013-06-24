package com.pannous.es.reindex;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * Refeeds all the documents which matches the type and the (optional) query.
 *
 * @author Peter Karich
 */
public class ReIndexAction extends BaseRestHandler {

    @Inject
    public ReIndexAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        if (controller != null) {
            // Define REST endpoints to do a reindex
            controller.registerHandler(PUT, "/{index}/{type}/_reindex", this);
            controller.registerHandler(POST, "/{index}/{type}/_reindex", this);
        }
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel) {
        handleRequest(request, channel, null, false);
    }

    public void handleRequest(RestRequest request, RestChannel channel, String newTypeOverride, boolean internalCall) {
        logger.info("ReIndexAction.handleRequest [{}]", request.params());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String newIndexName = request.param("index");
            String searchIndexName = request.param("searchIndex");
            if (searchIndexName == null || searchIndexName.isEmpty())
                searchIndexName = newIndexName;

            String newType = newTypeOverride != null ? newTypeOverride : request.param("type");
            String searchType = newTypeOverride != null ? newTypeOverride : request.param("searchType");
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

            if (!internalCall)
                channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            if (!internalCall) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, ex));
                } catch (Exception ex2) {
                    logger.error("problem while rolling index", ex2);
                }
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    public SearchRequestBuilder createScrollSearch(String oldIndexName, String oldType, String filter,
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

    public Thread reindex(MySearchResponse rsp, String newIndex, String newType, boolean withVersion,
                          float waitSeconds) {

        Indexer indexer = new Indexer(client, callback(rsp.hits()), rsp, newIndex, newType, withVersion, waitSeconds);
        Thread indexerThread = EsExecutors.daemonThreadFactory(settings, this.getClass().getCanonicalName()).newThread(indexer);
        indexerThread.start();

        return indexerThread;
    }

    protected HitsCallback callback(MySearchHits hits) {
        return new HitsCallback();
    }
}
