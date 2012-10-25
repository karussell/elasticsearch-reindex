package com.pannous.es.rollindex;

import java.util.Map;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class RollActionTests extends AbstractNodesTests {

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
        // TODO is client null in @BeforeTest?
        client.admin().indices().delete(new DeleteIndexRequest()).actionGet();
    }

    @Test public void rollingIndex() throws Exception {
        deleteAll();
        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RollAction action = new RollAction(emptySettings, client, new RestController(emptySettings)) {
            @Override public DateTimeFormatter createFormatter() {
                // use millisecond change for test
                return DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss-S");
            }
        };

        String rollIndexTag = action.getRoll("tweets");
        String searchIndex = action.getSearch("tweets");
        String feedIndex = action.getFeed("tweets");
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(1));
        assertThat(action.getAliases(searchIndex).size(), equalTo(1));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        // TODO sleep is necessary to ensure index name change        
        Thread.sleep(20);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(2));
        assertThat(action.getAliases(searchIndex).size(), equalTo(2));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(20);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(3));
        assertThat(action.getAliases(searchIndex).size(), equalTo(3));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(20);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(4));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(20);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(4));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(20);
        action.rollIndex("tweets", 4, 3);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(3));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));
    }

    @Test public void rollingIndex2() throws Exception {
        deleteAll();
        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RollAction action = new RollAction(emptySettings, client, new RestController(emptySettings)) {
            @Override public DateTimeFormatter createFormatter() {
                // use millisecond change for test
                return DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss-S");
            }
        };

        Map<String, Object> result = action.rollIndex("tweets", 2, 1);
        String newIndex = result.get("created").toString();
        assertThat(((String) result.get("deleted")), isEmptyString());

        Thread.sleep(20);
        result = action.rollIndex("tweets", 2, 1);
        assertThat(((String) result.get("deleted")), isEmptyString());

        Thread.sleep(20);
        result = action.rollIndex("tweets", 2, 1);
        assertThat(((String) result.get("deleted")), isEmptyString());
        assertThat(((String) result.get("closed")), equalTo(newIndex));
    }
}
