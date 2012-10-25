package com.pannous.es.rollindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.json.JsonXContent;
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
 * @see issue 1500 https://github.com/elasticsearch/elasticsearch/issues/1500
 *
 * Only indices with the rolling alias are involved into rolling.
 * @author Peter Karich
 */
public class RollAction extends BaseRestHandler {

    private String feedEnd = "feed";
    private String searchEnd = "search";
    // helper index
    private String rollEnd = "roll";

    @Inject public RollAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a roll further and to change the create-index-settings!        
        controller.registerHandler(PUT, "/{index}/_rollindex", this);
        controller.registerHandler(POST, "/{index}/_rollindex", this);
        logger.info("RollAction constructor [{}]", settings.toString());
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("RollAction.handleRequest [{}]", request.toString());
        try {
            XContentBuilder builder = restContentBuilder(request);
            String indexName = request.param("index", "");
            int searchIndices = request.paramAsInt("searchIndices", 1);
            int rollIndices = request.paramAsInt("rollIndices", 1);
            boolean deleteAfterRoll = request.paramAsBoolean("deleteAfterRoll", false);

            int newIndexShards = request.paramAsInt("newIndexShards", 2);
            int newIndexReplicas = request.paramAsInt("newIndexReplicas", 1);
            String newIndexRefresh = request.param("newIndexRefresh", "10s");

            CreateIndexRequest req;
            if (request.hasContent())
                req = new CreateIndexRequest("").source(request.contentAsString());
            else
                req = new CreateIndexRequest("").settings(toSettings(createIndexSettings(newIndexShards, newIndexReplicas, newIndexRefresh).string()));

            Map<String, Object> map = rollIndex(indexName, rollIndices, searchIndices,
                    deleteAfterRoll, req);

            builder.startObject();
            for (Entry<String, Object> e : map.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }

    public DateTimeFormatter createFormatter() {
        return DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm");
    }

    public Map<String, Object> rollIndex(String indexName, int maxRollIndices, int maxSearchIndices) {
        try {
            return rollIndex(indexName, maxRollIndices, maxSearchIndices, false,
                    new CreateIndexRequest("").settings(toSettings(createIndexSettings(2, 1, "10s").string())));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    Settings toSettings(String str) {
        return ImmutableSettings.settingsBuilder().loadFromSource(str).build();
    }

    // TODO make client calls async, see RestCreateIndexAction
    public Map<String, Object> rollIndex(String indexName, int maxRollIndices, int maxSearchIndices,
            boolean deleteAfterRoll, CreateIndexRequest request) {
        String rollAlias = getRoll(indexName);
        DateTimeFormatter formatter = createFormatter();
        if (maxRollIndices < 1 || maxSearchIndices < 1)
            throw new RuntimeException("remaining indices, search indices and feeding indices must be at least 1");
        if (maxSearchIndices > maxRollIndices)
            throw new RuntimeException("rollIndices must be higher or equal to searchIndices");

        // get old aliases
        Map<String, AliasMetaData> allRollingAliases = getAliases(rollAlias);

        // always create new index and append aliases
        String searchAlias = getSearch(indexName);
        String feedAlias = getFeed(indexName);
        String newIndexName = indexName + "_" + formatter.print(System.currentTimeMillis());

        client.admin().indices().create(request.index(newIndexName)).actionGet();
        addAlias(newIndexName, searchAlias);
        addAlias(newIndexName, rollAlias);

        String deletedIndices = "";
        String removedAlias = "";
        String closedIndices = "";
        String oldFeedIndexName = null;
        if (allRollingAliases.isEmpty()) {
            // do nothing for now
        } else {
            TreeMap<Long, String> sortedIndices = new TreeMap<Long, String>(reverseSorter);
            // Map<String, String> indexToConcrete = new HashMap<String, String>();
            String[] concreteIndices = getConcreteIndices(allRollingAliases.keySet());
            logger.info("aliases:{}, indices:{}", allRollingAliases, Arrays.toString(concreteIndices));
            for (String index : concreteIndices) {
                int pos = index.indexOf("_");
                if (pos < 0)
                    throw new IllegalStateException("index " + index + " is not in the format " + formatter);

                String indexDateStr = index.substring(pos + 1);
                Long timeLong;
                try {
                    timeLong = formatter.parseMillis(indexDateStr);
                } catch (Exception ex) {
                    throw new IllegalStateException("index " + index + " is not in the format " + formatter + " error:" + ex.getMessage());
                }
                String old = sortedIndices.put(timeLong, index);
                if (old != null)
                    throw new IllegalStateException("Indices with the identical date are not supported! " + old + " vs. " + index);
            }
            int counter = 1;
            Iterator<String> indexIter = sortedIndices.values().iterator();

            while (indexIter.hasNext()) {
                String currentIndexName = indexIter.next();
                if (counter >= maxRollIndices) {
                    if (deleteAfterRoll) {
                        deleteIndex(currentIndexName);
                        deletedIndices += currentIndexName + " ";
                    } else {
                        removeAlias(currentIndexName, rollAlias);
                        removeAlias(currentIndexName, searchAlias);
                        closeIndex(currentIndexName);
                        closedIndices += currentIndexName + " ";
                        removedAlias += currentIndexName + " ";
                        removedAlias += currentIndexName + " ";
                    }
                    // close/delete all the older indices
                    continue;
                }

                if (counter == 1)
                    oldFeedIndexName = currentIndexName;

                if (counter >= maxSearchIndices) {
                    removeAlias(currentIndexName, searchAlias);
                    removedAlias += currentIndexName + " ";
                }

                counter++;
            }
        }
        if (oldFeedIndexName != null)
            moveAlias(oldFeedIndexName, newIndexName, feedAlias);
        else
            addAlias(newIndexName, feedAlias);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("created", newIndexName);
        map.put("deleted", deletedIndices.trim());
        map.put("closed", closedIndices.trim());
        map.put("removedAlias", removedAlias.trim());
        return map;
    }

    XContentBuilder createIndexSettings(int shards, int replicas, String refresh) {
        try {
            XContentBuilder createIndexSettings = JsonXContent.contentBuilder().startObject().
                    field("index.number_of_shards", shards).
                    field("index.number_of_replicas", replicas).
                    field("index.refresh_interval", refresh).endObject();
            return createIndexSettings;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void deleteIndex(String indexName) {
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }

    public void closeIndex(String indexName) {
        client.admin().indices().close(new CloseIndexRequest(indexName)).actionGet();
    }

    public void addAlias(String indexName, String alias) {
        client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(indexName, alias)).actionGet();
    }

    public void removeAlias(String indexName, String alias) {
        client.admin().indices().aliases(new IndicesAliasesRequest().removeAlias(indexName, alias)).actionGet();
    }

    public void moveAlias(String oldIndexName, String newIndexName, String alias) {
        client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(newIndexName, alias).
                removeAlias(oldIndexName, alias)).actionGet();
    }

    public Map<String, AliasMetaData> getAliases(String index) {
        Map<String, AliasMetaData> md = client.admin().cluster().state(new ClusterStateRequest()).
                actionGet().getState().getMetaData().aliases().get(index);
        if (md == null)
            return Collections.emptyMap();

        return md;
    }
    private static Comparator<Long> reverseSorter = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return -o1.compareTo(o2);
        }
    };

    public String[] getConcreteIndices(Set<String> set) {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().
                getMetaData().concreteIndices(set.toArray(new String[set.size()]));
    }

    String getRoll(String indexName) {
        return indexName + "_" + rollEnd;
    }

    String getFeed(String indexName) {
        if (feedEnd.isEmpty())
            return indexName;
        return indexName + "_" + feedEnd;
    }

    String getSearch(String indexName) {
        if (searchEnd.isEmpty())
            return indexName;
        return indexName + "_" + searchEnd;
    }
}
