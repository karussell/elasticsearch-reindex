package com.pannous.es.reindex;

public class ReIndexActionJsonTest extends ReIndexActionTester {

    @Override
    protected MySearchResponse scrollSearch(String searchIndex, String type, String filter, int hits,
            boolean withVersion, int keepMinutes) {
        // TODO you need to shutdown all local instances to run this test
        return new MySearchResponseJson("localhost", 9200, searchIndex, type, filter, "", hits,
                withVersion, keepMinutes);
    }
}
