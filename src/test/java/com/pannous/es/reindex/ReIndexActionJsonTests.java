package com.pannous.es.reindex;

public class ReIndexActionJsonTests extends ReIndexActionTester {

    @Override
    protected MySearchResponse scrollSearch(String oldIndex, String type, String query, int hits,
            boolean withVersion, int keepMinutes) {
        return new MySearchResponseJson("localhost", 9200, oldIndex, type, query, hits,
                withVersion, keepMinutes);
    }
}
