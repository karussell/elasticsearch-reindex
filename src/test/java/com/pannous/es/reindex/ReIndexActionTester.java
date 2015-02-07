package com.pannous.es.reindex;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * This test will be called from the ElasticSearch and from the 'JSON'
 * implementation. (to avoid code duplication)
 *
 * @author Peter Karich
 */
public abstract class ReIndexActionTester extends AbstractNodesTests {

    protected Client client;
    protected ReIndexAction action;
    // protected String indexNames = "oldtweets,tweets";

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = client("node1");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @BeforeMethod public void setUp() {
        client.admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        action = new ReIndexAction(emptySettings, client, new RestController(emptySettings));
    }

    protected MySearchResponse scrollSearch(String searchIndex, String searchType, String query) {
        return scrollSearch(searchIndex, searchType, query, 10, false, 1);
    }

    protected abstract MySearchResponse scrollSearch(String searchIndex, String searchType,
            String filter, int hits, boolean withVersion, int keepMinutes);

    @Test public void reindexAll() throws Exception {
        add("oldtweets", "tweet", null, "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", null, "{ \"name\" : \"peter ä test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));

        int res = action.reindex(scrollSearch("oldtweets", "tweet", ""), "tweets", "tweet", false, 0, client);
        assertThat(res, equalTo(2));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(2L));

        // now check if content was correctly streamed and saved
        SearchResponse sr = client.prepareSearch("tweets").
                addSort("count", SortOrder.ASC).execute().actionGet();
        assertThat(sr.getHits().hits().length, equalTo(2));
        assertThat(new JSONObject(sr.getHits().hits()[0].sourceAsString()).getString("name"), equalTo("hello world"));
        assertThat(new JSONObject(sr.getHits().hits()[1].sourceAsString()).getString("name"), equalTo("peter ä test"));
    }

    @Test public void reindexAllPartial() throws Exception {
        add("oldtweets", "tweet", null, "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", null, "{ \"name\" : \"peter test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));
        int res = action.reindex(scrollSearch("oldtweets", "tweet", "{ \"term\": { \"count\" : 2} }"), "tweets", "tweet", false, 0, client);
        assertThat(res, equalTo(1));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(1L));
        SearchResponse sr = client.prepareSearch("tweets").execute().actionGet();
        assertThat(sr.getHits().hits().length, equalTo(1));
        assertThat(new JSONObject(sr.getHits().hits()[0].sourceAsString()).getString("name"), equalTo("peter test"));
    }

    @Test public void reindexChilds() throws Exception {
         String parent = add("oldtweets", "tweet", null, "{ \"name\" : \"hello world\", \"count\" : 1}");
         // update the mapping settings for oldtweets childs (i.e retweet type)
         client.admin().indices().preparePutMapping().setIndices("oldtweets").setType("retweet").setSource("{\"retweet\": { \"_parent\": { \"type\": \"tweet\" }, \"_routing\": { \"required\": true }, \"properties\": { \"name\": { \"type\": \"string\" }, \"count\": { \"type\": \"long\" } } }}").execute().actionGet();
         String child = add("oldtweets", "retweet", parent, "{ \"name\" : \"RE: hello world\", \"count\" : 1, \"_parent\" : \"" + parent + "\"}");
         refresh("oldtweets");
         assertThat(count("oldtweets"), equalTo(2L));

         int res = action.reindex(scrollSearch("oldtweets", "tweet", ""), "tweets", "tweet", false, 0, client);
         assertThat(res, equalTo(1));
         refresh("tweets");
         assertThat(count("tweets"), equalTo(1L));

         // update the mapping settings for oldtweets childs (i.e retweet type)
         client.admin().indices().preparePutMapping().setIndices("tweets").setType("retweet").setSource("{\"retweet\": { \"_parent\": { \"type\": \"tweet\" }, \"_routing\": { \"required\": true }, \"properties\": { \"name\": { \"type\": \"string\" }, \"count\": { \"type\": \"long\" } } }}").execute().actionGet();

         res = action.reindex(scrollSearch("oldtweets", "retweet", ""), "tweets", "retweet", false, 0, client);
         assertThat(res, equalTo(1));
         refresh("tweets");
         assertThat(count("tweets"), equalTo(2L));

        // now check if content was correctly streamed and saved
        SearchResponse parent_sr = client.prepareSearch("tweets").setTypes("tweet")
                .addSort("count", SortOrder.ASC).execute().actionGet();
        assertThat(parent_sr.getHits().hits().length, equalTo(1));
        String reindex_parent = parent_sr.getHits().hits()[0].id();

        SearchResponse child_sr = client.prepareSearch("tweets").setTypes("retweet").setRouting(reindex_parent).setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("_parent", reindex_parent)))
                .addSort("count", SortOrder.ASC).execute().actionGet();
        assertThat(child_sr.getHits().hits().length, equalTo(1));
    }

    @Test public void copyAliases() throws Exception {
        add("oldtweets", "tweet", null, "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("tweets", "tweet", null, "{ \"name\" : \"peter ä test\", \"count\" : 2}");
        IndicesAliasesRequest aReq = new IndicesAliasesRequest();
        aReq.addAlias("myalias", "oldtweets");
        client.admin().indices().aliases(aReq).actionGet();
        refresh("oldtweets");
        refresh("tweets");
        List<String> oldAliases = getAliasesNames("oldtweets");
        assertThat(oldAliases.size(), equalTo(1));
        assertThat(oldAliases.get(0), equalTo("myalias"));

        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RestController contrl = new RestController(emptySettings);
        ReIndexWithCreate action = new ReIndexWithCreate(emptySettings, client, contrl);

        Method copyAliases = action.getClass().getDeclaredMethod("copyAliases", String.class, String.class, Boolean.class, Client.class);
        copyAliases.setAccessible(true);
        copyAliases.invoke(action, "tweets", "oldtweets", false, client);
        List<String> createdAliases = getAliasesNames("tweets");
        assertThat(oldAliases, equalTo(createdAliases));
    }

    private String add(String index, String type, String routing, String json) {
        IndexRequestBuilder req =  client.prepareIndex(index, type).setSource(json);
        if (routing != null) 
            req.setRouting(routing);
        
        IndexResponse rsp = req.execute().actionGet();
        return rsp.getId();
    }

    private void refresh(String index) {
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    private List<String> getAliasesNames(String index) {
        IndexMetaData meta = client.admin().cluster().state(new ClusterStateRequest()).
                actionGet().getState().metaData().index(index);
        List<String> aliases = new ArrayList<String>();
        if(meta != null && meta.aliases() != null) {
            for (ObjectCursor<String> oldAliasCursor : meta.aliases().keys()) {
                aliases.add(oldAliasCursor.value);
            }
        }
        return aliases;
    }

    private long count(String index) {
        return client.count(new CountRequest(index)).actionGet().getCount();
    }
}
