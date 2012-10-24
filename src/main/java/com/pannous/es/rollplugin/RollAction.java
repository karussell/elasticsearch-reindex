package com.pannous.es.rollplugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

/**
 * @see issue 1500 https://github.com/elasticsearch/elasticsearch/issues/1500
 *
 * @author Peter Karich
 */
public class RollAction extends BaseRestHandler {

    private static final String simpleDateString = "yyyy-MM-dd-HH-mm-ss";
    private XContentBuilder createIndexSettings;
    // TODO make complete createIndexSettings configurable
    private int createIndexShards = 2;
    private int createIndexReplicas = 2;
    private boolean deleteAfterRoll = true;
    private String feedEnd = "_feed";
    private String searchEnd = "_feed";
    // helper index
    private String rollEnd = "_roll";

    @Inject public RollAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a roll further and to change the create-index-settings!
        // controller.registerHandler(PUT, "/_rollindex", this);
        logger.info("RollAction constructor [{}]", settings.toString());
    }

    public void handleRequest(RestRequest rr, RestChannel rc) {
        logger.info("RollAction.handleRequest [{}]", rr.toString());
    }

    public DateTimeFormatter createFormatter() {
        return DateTimeFormat.forPattern(simpleDateString);
    }

    public String rollIndex(String indexName, int maxRollIndices, int maxSearchIndices) {
        String rollAlias = getRoll(indexName);

        DateTimeFormatter formatter = createFormatter();
        if (maxRollIndices < 1 || maxSearchIndices < 1)
            throw new RuntimeException("remaining indices, search indices and feeding indices must be at least 1");

        // get old aliases
        Map<String, AliasMetaData> allRollingAliases = getAliases(rollAlias);

        // always create new index and append aliases
        String searchAlias = getSearch(indexName);
        String feedAlias = getFeed(indexName);
        String newIndexName = indexName + "_" + formatter.print(System.currentTimeMillis());

        createIndex(newIndexName);
        addAlias(newIndexName, searchAlias);
        addAlias(newIndexName, rollAlias);

        String oldFeedIndexName = null;
        if (allRollingAliases.isEmpty()) {
            // do nothing for now
        } else {
            TreeMap<Long, String> sortedIndices = new TreeMap<Long, String>(reverseSorter);
            String[] concreteIndices = getConcreteIndices(allRollingAliases.keySet());
            logger.info("aliases:{}, indices:{}", allRollingAliases, Arrays.toString(concreteIndices));
            for (String index : concreteIndices) {
                int pos = index.indexOf("_");
                if (pos < 0)
                    throw new IllegalStateException("index " + index + " is not in the format " + simpleDateString);

                String indexDateStr = index.substring(pos + 1);
                Long timeLong;
                try {
                    timeLong = formatter.parseMillis(indexDateStr);
                } catch (Exception ex) {
                    throw new IllegalStateException("index " + index + " is not in the format " + simpleDateString + " error:" + ex.getMessage());
                }
                String old = sortedIndices.put(timeLong, index);
                if (old != null)
                    throw new IllegalStateException("indices with the identical date are not supported " + old + " vs. " + index);
            }
            int counter = 1;
            Iterator<String> indexIter = sortedIndices.values().iterator();

            while (indexIter.hasNext()) {
                String currentIndexName = indexIter.next();
                if (counter >= maxRollIndices) {
                    if (deleteAfterRoll)
                        deleteIndex(currentIndexName);
                    else
                        closeIndex(currentIndexName);
                    // close/delete all the older indices
                    continue;
                }

                if (counter == 1)
                    oldFeedIndexName = currentIndexName;

                if (counter >= maxSearchIndices)
                    removeAlias(currentIndexName, searchAlias);

                counter++;
            }
        }
        if (oldFeedIndexName != null)
            moveAlias(oldFeedIndexName, newIndexName, feedAlias);
        else
            addAlias(newIndexName, feedAlias);

        return newIndexName;
    }

    public void createIndex(String indexName) {
        client.admin().indices().create(new CreateIndexRequest(indexName).settings(createIndexSettings())).actionGet();
    }

    public XContentBuilder createIndexSettings() {
        if (createIndexSettings == null) {
            try {
                createIndexSettings = JsonXContent.contentBuilder().startObject().
                        field("index.number_of_shards", createIndexShards).
                        field("index.number_of_replicas", createIndexReplicas).
                        field("index.refresh_interval", "10s").
                        field("index.merge.policy.merge_factor", 10).endObject();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        return createIndexSettings;
    }

    public void setCreateIndexSettings(XContentBuilder createIndexSettings) {
        this.createIndexSettings = createIndexSettings;
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
        return indexName + rollEnd;
    }

    String getFeed(String indexName) {
        return indexName + feedEnd;
    }

    String getSearch(String indexName) {
        return indexName + searchEnd;
    }
}
