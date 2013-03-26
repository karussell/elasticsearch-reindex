package com.pannous.es.reindex;

import org.elasticsearch.action.search.SearchRequestBuilder;

public class ReIndexActionESTest extends ReIndexActionTester {

    @Override
    protected MySearchResponse scrollSearch(String index, String type, String query, int hits,
            boolean withVersion, int keepMinutes) {
        SearchRequestBuilder srb = action.createScrollSearch(index, type, query, hits, withVersion, keepMinutes);
        return new MySearchResponseES(client, srb.execute().actionGet(), keepMinutes);
    }
}
