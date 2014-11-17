package com.pannous.es.reindex;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestController;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class to use the reindex plugin as rewrite/refeed plugin - directly from
 * java.
 *
 * @author Peter Karich
 */
public class ExampleUsage {

    private final static String charset = "UTF-8";

    public static void main(String[] args) {
        String searchHost = "1.1.1.1";
        int searchPort = 9300;
        String searchIndexName = "search_index";
        String searchType = "some_type";
        String newIndexName = "feed_index";
        String newType = "some_type";
        // String filter = "{ 'term' : {'locale' : 'de'} }".replaceAll("'", "\"");
        String filter = "{ 'query' : {'query_string' : { 'query' : 'text:blup*'} } }".replaceAll("'", "\"");
        String basicAuthCredentials = "base64_ifrequried=";
        boolean withVersion = false;
        final int hitsPerPage = 500;
        float waitInSeconds = 0.1f;
        // increase if you have lots of things to update
        int keepTimeInMinutes = 90;
        String cluster = "your_production_cluster_name";

        boolean local = false;
        if (local) {
            cluster = "elasticsearch";
            searchHost = "localhost";
            basicAuthCredentials = "base64_ifrequried=";
        }

        Logger.getLogger("test").info("querying " + searchHost + ":" + searchPort
                + " at " + searchIndexName + " with " + basicAuthCredentials);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", cluster).build();
        Client client = new TransportClient(settings).
                addTransportAddress(new InetSocketTransportAddress(searchHost, searchPort));

        Settings emptySettings = ImmutableSettings.settingsBuilder().build();
        RestController contrl = new RestController(emptySettings);
        ReIndexAction action = new ReIndexAction(emptySettings, client, contrl) {
            @Override protected MySearchHits callback(MySearchHits hits) {
                SimpleList res = new SimpleList(hitsPerPage, hits.totalHits());
                for (MySearchHit h : hits.getHits()) {
                    try {
                        String str = new String(h.source(), charset);
                        RewriteSearchHit newHit = new RewriteSearchHit(h.id(), h.parent(), h.version(), str);
                        String someField = newHit.get("some_field");
                        if (someField.contains("some content")) {
                            newHit.put("some_field", "IT WORKS!");
                        }

                        res.add(newHit);
                    } catch (UnsupportedEncodingException ex) {
                        throw new RuntimeException(ex);
                    }
                }
                return res;
            }
        };
        // first query, further scroll-queries in reindex!
        SearchRequestBuilder srb = action.createScrollSearch(searchIndexName, searchType, filter,
                hitsPerPage, withVersion, keepTimeInMinutes, client);
        SearchResponse sr = srb.execute().actionGet();
        MySearchResponse rsp = new MySearchResponseES(client, sr, keepTimeInMinutes);

        // now feed and call callback
        action.reindex(rsp, newIndexName, newType, withVersion, waitInSeconds, client);

        client.close();
    }

    public static class SimpleList implements MySearchHits {

        long totalHits;
        List<MySearchHit> hits;

        public SimpleList(int size, long total) {
            hits = new ArrayList<MySearchHit>(size);
            totalHits = total;
        }

        public void add(MySearchHit hit) {
            hits.add(hit);
        }

        @Override public Iterable<MySearchHit> getHits() {
            return hits;
        }

        @Override
        public long totalHits() {
            return totalHits;
        }
    }

    public static class RewriteSearchHit implements MySearchHit {

        String id;
        String parent;
        long version;
        JSONObject json;

        public RewriteSearchHit(String id, String parent, long version, String jsonStr) {
            this.id = id;
            this.version = version;
            this.parent = parent;
            try {
                json = new JSONObject(jsonStr);
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        public String get(String key) {
            try {
                if (!json.has(key))
                    return "";
                String val = json.getString(key);
                if (val == null)
                    return "";
                return val;
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        public JSONObject put(String key, Object obj) {
            try {
                return json.put(key, obj);
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override public String id() {
            return id;
        }
        
        @Override public String parent() {
            return parent;
        }
        @Override public long version() {
            return version;
        }

        @Override public byte[] source() {
            try {
                return json.toString().getBytes(charset);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
