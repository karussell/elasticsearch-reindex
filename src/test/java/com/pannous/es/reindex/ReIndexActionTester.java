package com.pannous.es.reindex;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.search.sort.SortOrder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;

/**
 * This test will be called from the ElasticSearch and from the 'JSON'
 * implementation. (to avoid code duplication)
 *
 * @author Peter Karich
 */
public abstract class ReIndexActionTester extends AbstractNodesTests {

    protected Client client;
    protected ReIndexAction action;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = client("node1");
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @BeforeMethod public void setUp() {
        client.admin().indices().delete(new DeleteIndexRequest()).actionGet();
        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        action = new ReIndexAction(emptySettings, client, new RestController(emptySettings));
    }

    protected MySearchResponse scrollSearch(String searchIndex, String searchType, String query) {
        return scrollSearch(searchIndex, searchType, query, 10, false, 1);
    }

    protected abstract MySearchResponse scrollSearch(String searchIndex, String searchType,
            String filter, int hits, boolean withVersion, int keepMinutes);

    @Test public void reindexAll() throws Exception {
        add("oldtweets", "tweet", "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", "{ \"name\" : \"peter ä test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));

        int res = action.reindex(scrollSearch("oldtweets", "tweet", ""), "tweets", "tweet", false, 0);
        assertThat(res, equalTo(2));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(2L));

        // now check if content was correctly streamed and saved
        SearchResponse sr = client.prepareSearch("tweets").
                addSort("count", SortOrder.ASC).execute().actionGet();
        assertThat(sr.hits().hits().length, equalTo(2));
        assertThat(new JSONObject(sr.hits().hits()[0].sourceAsString()).getString("name"), equalTo("hello world"));
        assertThat(new JSONObject(sr.hits().hits()[1].sourceAsString()).getString("name"), equalTo("peter ä test"));
    }

    @Test public void reindexAllPartial() throws Exception {
        add("oldtweets", "tweet", "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", "{ \"name\" : \"peter test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));
        int res = action.reindex(scrollSearch("oldtweets", "tweet", "{ \"term\": { \"count\" : 2} }"), "tweets", "tweet", false, 0);
        assertThat(res, equalTo(1));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(1L));
        SearchResponse sr = client.prepareSearch("tweets").execute().actionGet();
        assertThat(sr.hits().hits().length, equalTo(1));
        assertThat(new JSONObject(sr.hits().hits()[0].sourceAsString()).getString("name"), equalTo("peter test"));
    }

    private void add(String index, String type, String json) {
        client.prepareIndex(index, type).setSource(json).execute().actionGet();
    }

    private void refresh(String index) {
        client.admin().indices().refresh(new RefreshRequest(index)).actionGet();
    }

    private long count(String index) {
        return client.count(new CountRequest(index)).actionGet().count();
    }
}
