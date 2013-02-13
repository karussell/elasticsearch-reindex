package com.pannous.es.reindex;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
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
public class ReIndexAllAction extends BaseRestHandler {

    private final ReIndexAction reindexAction;

    @Inject public ReIndexAllAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a reindex
        controller.registerHandler(PUT, "/_reindexall", this);
        controller.registerHandler(POST, "/_reindexall", this);
        reindexAction = new ReIndexAction(settings, client, controller);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexAllAction.handleRequest [{}]", request.toString());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String typesStr = request.param("types", "_all");
            String indicesStr = request.param("indices", "_all");
            boolean withVersion = request.paramAsBoolean("withVersion", false);
            int keepTimeInMinutes = request.paramAsInt("keepTimeInMinutes", 30);
            int hitsPerPage = request.paramAsInt("hitsPerPage", 1000);
            int waitInSeconds = request.paramAsInt("waitInSeconds", 0);

            MetaData metaData = client.admin().cluster().state(new ClusterStateRequest()).
                    actionGet().state().metaData();
            Map<String, IndexMetaData> indices;
            if (indicesStr.equals("_all"))
                indices = metaData.indices();
            else {
                indices = Collections.singletonMap(indicesStr, metaData.index(indicesStr));
            }

            for (Entry<String, IndexMetaData> e : indices.entrySet()) {
                String indexName = e.getKey();
                IndexMetaData data = e.getValue();
                Set<String> types;
                if (typesStr.equals("_all"))
                    types = data.getMappings().keySet();
                else {
                    if (!data.getMappings().containsKey(typesStr)) {
                        logger.info("skipping type " + typesStr + " for " + indexName);
                        continue;
                    }
                    types = Collections.singleton(typesStr);
                }
                for (String type : types) {
                    SearchRequestBuilder srb = reindexAction.createScrollSearch(indexName, type, null,
                            hitsPerPage, withVersion, keepTimeInMinutes);
                    SearchResponse sr = srb.execute().actionGet();
                    MySearchResponseES rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);
                    reindexAction.reindex(rsp, indexName, type, withVersion, waitInSeconds);
                    logger.info("Finished reindexing of index " + indexName + ", type " + type);
                }
            }
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }
}
