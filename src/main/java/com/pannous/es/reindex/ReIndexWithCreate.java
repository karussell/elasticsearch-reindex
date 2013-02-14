package com.pannous.es.reindex;

import java.io.IOException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
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

        reindexAction = new ReIndexAction(settings, client, controller);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("ReIndexWithCreate.handleRequest [{}]", request.toString());

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
            createIdenticalIndex(searchIndexName, type, newIndexName, newShards);
        } catch (Exception ex) {
            String str = "Problem while creating index " + newIndexName + " from " + searchIndexName + " " + ex.getMessage();
            logger.error(str, ex);
            channel.sendResponse(new StringRestResponse(RestStatus.INTERNAL_SERVER_ERROR, str));
            return;
        }
        copyAliases(request);
        reindexAction.handleRequest(request, channel);

        boolean delete = request.paramAsBoolean("delete", false);
        if (delete) {
            long oldCount = client.count(new CountRequest(searchIndexName)).actionGet().count();
            long newCount = client.count(new CountRequest(newIndexName)).actionGet().count();
            if (oldCount == newCount) {
                logger.info("deleting " + searchIndexName);
                client.admin().indices().delete(new DeleteIndexRequest(searchIndexName)).actionGet();
            }
        }

        boolean aliasIncludeIndex = request.paramAsBoolean("addOldIndexAsAlias", false);
        if (aliasIncludeIndex)
            addOldIndexAsAlias(request);
    }

    /**
     * Creates a new index out of the settings from the old index.
     */
    private void createIdenticalIndex(String oldIndex, String type,
            String newIndex, int newIndexShards) throws IOException {
        IndexMetaData indexData = client.admin().cluster().state(new ClusterStateRequest()).
                actionGet().state().metaData().indices().get(oldIndex);
        Settings searchIndexSettings = indexData.settings();
        ImmutableSettings.Builder settingBuilder = ImmutableSettings.settingsBuilder().put(searchIndexSettings);
        if (newIndexShards > 0)
            settingBuilder.put("index.number_of_shards", newIndexShards);
        MappingMetaData mappingMeta = indexData.mapping(type);
        CreateIndexRequest createReq = new CreateIndexRequest(newIndex).
                mapping(type, mappingMeta.sourceAsMap()).
                settings(settingBuilder.build());
        client.admin().indices().create(createReq).actionGet();
    }

    private void copyAliases(RestRequest request) {
        String index = request.param("index");
        String searchIndexName = request.param("searchIndex");
        IndexMetaData meta = client.admin().cluster().state(new ClusterStateRequest()).actionGet().state().metaData().index(searchIndexName);
        IndicesAliasesRequest aReq = new IndicesAliasesRequest();
        for (String oldAlias : meta.aliases().keySet()) {
            aReq.addAlias(index, oldAlias);
        }
        client.admin().indices().aliases(aReq).actionGet();
    }

    private void addOldIndexAsAlias(RestRequest request) {
        String searchIndexName = request.param("searchIndex");
        String index = request.param("index");

        if (client.admin().indices().exists(new IndicesExistsRequest(searchIndexName)).actionGet().exists()) {
            logger.warn("Couldn't add old indexName as alias as index still exists");
            return;
        }

        IndicesAliasesRequest aReq = new IndicesAliasesRequest();
        aReq.addAlias(index, searchIndexName);
        client.admin().indices().aliases(aReq).actionGet();
    }
}
