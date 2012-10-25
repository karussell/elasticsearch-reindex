package com.pannous.es.rollindex;

import com.pannous.es.rollindex.RollAction;
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
import static org.hamcrest.Matchers.equalTo;

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

    @Test public void rollingIndex() throws Exception {
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
}
