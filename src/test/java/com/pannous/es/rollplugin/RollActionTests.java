package com.pannous.es.rollplugin;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RollActionTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void rollingIndex() throws Exception {
        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RollAction action = new RollAction(emptySettings, client, new RestController(emptySettings));
        String rollIndexTag = action.getRoll("tweets");
        String searchIndex = action.getSearch("tweets");
        String feedIndex = action.getFeed("tweets");
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(1));
        assertThat(action.getAliases(searchIndex).size(), equalTo(1));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        // TODO avoid sleep! 
        // -> at the moment we need this so that the index name changes -> overwrite create formatter!
        //
        Thread.sleep(1000);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(2));
        assertThat(action.getAliases(searchIndex).size(), equalTo(2));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(1000);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(3));
        assertThat(action.getAliases(searchIndex).size(), equalTo(3));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(1000);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(4));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(1000);
        action.rollIndex("tweets", 4, 4);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(4));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));

        Thread.sleep(1000);
        action.rollIndex("tweets", 4, 3);
        assertThat(action.getAliases(rollIndexTag).size(), equalTo(4));
        assertThat(action.getAliases(searchIndex).size(), equalTo(3));
        assertThat(action.getAliases(feedIndex).size(), equalTo(1));
    }
}
