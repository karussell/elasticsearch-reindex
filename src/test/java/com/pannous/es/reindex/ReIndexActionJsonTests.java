package com.pannous.es.reindex;

public class ReIndexActionJsonTests extends ReIndexActionTester {

    @Override
    protected MySearchResponse scrollSearch(String searchIndex, String type, String query, int hits,
            boolean withVersion, int keepMinutes) {
        // TODO you need to shutdown all local instances to run this test
        return new MySearchResponseJson("localhost", 9200, searchIndex, type, query, hits,
                withVersion, keepMinutes);
    }
}
