package com.pannous.es.reindex;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import static org.elasticsearch.rest.RestRequest.Method.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.StringRestResponse;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * @author Peter Karich
 */
public class ReIndexWithCreate extends BaseRestHandler {

    private ReIndexAction reindexAction;

    @Inject public ReIndexWithCreate(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a reindex
        controller.registerHandler(PUT, "/_reindex", this);
        controller.registerHandler(POST, "/_reindex", this);

        // give null controller as argument to avoid registering twice
        // which would lead to an assert exception
        reindexAction = new ReIndexAction(settings, client, null);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexWithCreate.handleRequest [{}]", request.toString());
        try {
            XContentBuilder builder = restContentBuilder(request);
            // required parameters
            String newIndexName = request.param("index");
            if (newIndexName.isEmpty()) {
                channel.sendResponse(new StringRestResponse(RestStatus.EXPECTATION_FAILED, "parameter index missing"));
                return;
            }
            String type = request.param("type", "");
            if (type.isEmpty()) {
                channel.sendResponse(new StringRestResponse(RestStatus.EXPECTATION_FAILED, "parameter type missing"));
                return;
            }
            String searchIndexName = request.param("searchIndex");
            if (searchIndexName.isEmpty()) {
                channel.sendResponse(new StringRestResponse(RestStatus.EXPECTATION_FAILED, "parameter searchIndex missing"));
                return;
            }
            int newShards = request.paramAsInt("newIndexShards", -1);
            try {
                if(client.admin().indices().exists(new IndicesExistsRequest(newIndexName)).actionGet().isExists()) {
                    logger.info("target index already exists, skip creation: " + newIndexName);
                }
                else {
                    createIdenticalIndex(searchIndexName, type, newIndexName, newShards);
                }
            } catch (Exception ex) {
                String str = "Problem while creating index " + newIndexName + " from " + searchIndexName + " " + ex.getMessage();
                logger.error(str, ex);
                channel.sendResponse(new StringRestResponse(RestStatus.INTERNAL_SERVER_ERROR, str));
                return;
            }

            // TODO: what if queries goes to the old index while we reindexed?
            // now reindex
        
            if(type.equals("*")) {

                IndexMetaData indexData = client.admin().cluster().state(new ClusterStateRequest()).
                        actionGet().getState().metaData().indices().get(searchIndexName);
                Settings searchIndexSettings = indexData.settings();

                for(Map.Entry<String, MappingMetaData> me : indexData.mappings().entrySet()) {
                    reindexAction.handleRequest(request, channel, me.getKey(), true);
                }
            }
            else {
                reindexAction.handleRequest(request, channel, type, true);
            }

            boolean delete = request.paramAsBoolean("delete", false);
            if (delete) {
            
                // make sure to refresh the index here
                // (e.g. the index may be paused or refreshing with a very long interval):
                logger.info("refreshing " + searchIndexName);
                client.admin().indices().refresh(new RefreshRequest(newIndexName)).actionGet();
            
                long oldCount = client.count(new CountRequest(searchIndexName)).actionGet().getCount();
                long newCount = client.count(new CountRequest(newIndexName)).actionGet().getCount();
                if (oldCount == newCount) {
                    logger.info("deleting " + searchIndexName);
                    client.admin().indices().delete(new DeleteIndexRequest(searchIndexName)).actionGet();
                }
            }

            boolean copyAliases = request.paramAsBoolean("copyAliases", false);
            if (copyAliases)
                copyAliases(request);
                
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
                
        } catch (Exception ex) { // also catch the RuntimeException thrown by ReIndexAction
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }

    /**
     * Creates a new index out of the settings from the old index.
     */
    private void createIdenticalIndex(String oldIndex, String type,
            String newIndex, int newIndexShards) throws IOException {
        IndexMetaData indexData = client.admin().cluster().state(new ClusterStateRequest()).
                actionGet().getState().metaData().indices().get(oldIndex);
        Settings searchIndexSettings = indexData.settings();
        ImmutableSettings.Builder settingBuilder = ImmutableSettings.settingsBuilder().put(searchIndexSettings);
        if (newIndexShards > 0)
            settingBuilder.put("index.number_of_shards", newIndexShards);
            
        CreateIndexRequest createReq;
        
        if(type.equals("*")) {
            createReq = new CreateIndexRequest(newIndex);
            for(Map.Entry<String, MappingMetaData> me : indexData.mappings().entrySet()) {
                createReq.mapping(me.getKey(), me.getValue().sourceAsMap());
            }
            createReq.settings(settingBuilder.build());
        }
        else {
            MappingMetaData mappingMeta = indexData.mapping(type);
            createReq = new CreateIndexRequest(newIndex).
                mapping(type, mappingMeta.sourceAsMap()).
                settings(settingBuilder.build());
        }

        client.admin().indices().create(createReq).actionGet();
    }

    private void copyAliases(RestRequest request) {
        String index = request.param("index");
        String searchIndexName = request.param("searchIndex");
        IndexMetaData meta = client.admin().cluster().state(new ClusterStateRequest()).
                actionGet().getState().metaData().index(searchIndexName);
        IndicesAliasesRequest aReq = new IndicesAliasesRequest();
        boolean empty = true;
        if(meta != null && meta.aliases() != null) {
            for (String oldAlias : meta.aliases().keySet()) {
                empty = false;
                aReq.addAlias(index, oldAlias);
            }
        }
        boolean aliasIncludeIndex = request.paramAsBoolean("addOldIndexAsAlias", false);
        if (aliasIncludeIndex) {
            if (client.admin().indices().exists(new IndicesExistsRequest(searchIndexName)).actionGet().isExists()) {
                logger.warn("Cannot add old index name (" + searchIndexName + ") as alias to index "
                        + index + " - as old index still exists");
            }
            else {
                aReq.addAlias(index, searchIndexName);
                empty = false;
            }
        }
        if(!empty) //!aReq.aliasActions().isEmpty())
            client.admin().indices().aliases(aReq).actionGet();
    }
}
