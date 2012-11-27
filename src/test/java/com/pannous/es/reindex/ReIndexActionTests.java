package com.pannous.es.reindex;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ReIndexActionTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    public void deleteAll() {
        // TODO client is null in @BeforeTest?
        client.admin().indices().delete(new DeleteIndexRequest()).actionGet();
    }

    @Test public void reindexAll() throws Exception {
        deleteAll();
        add("oldtweets", "tweet", "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", "{ \"name\" : \"peter test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));

        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        ReIndexAction action = new ReIndexAction(emptySettings, client, new RestController(emptySettings));
        SearchRequestBuilder srb = action.createSearch("oldtweets", "tweet", "", 10, true, 10);
        int res = action.reindex(createSearchResponseES(srb.execute().actionGet()), "tweets", "tweet", 10, false);
        assertThat(res, equalTo(2));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(2L));
    }

    @Test public void reindexAllPartial() throws Exception {
        deleteAll();
        add("oldtweets", "tweet", "{ \"name\" : \"hello world\", \"count\" : 1}");
        add("oldtweets", "tweet", "{ \"name\" : \"peter test\", \"count\" : 2}");
        refresh("oldtweets");
        assertThat(count("oldtweets"), equalTo(2L));

        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        ReIndexAction action = new ReIndexAction(emptySettings, client, new RestController(emptySettings));
        SearchRequestBuilder srb = action.createSearch("oldtweets", "tweet", "{ \"term\": { \"count\" : 2} }", 10, true, 10);
        int res = action.reindex(createSearchResponseES(srb.execute().actionGet()), "tweets", "tweet", 10, false);
        assertThat(res, equalTo(1));
        refresh("tweets");
        assertThat(count("tweets"), equalTo(1L));
    }

    MySearchResponse createSearchResponseES(SearchResponse sr) {
        return new MySearchResponseES(client, sr, 10);
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
