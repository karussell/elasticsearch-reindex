/*
 *  Copyright 2012 Peter Karich info@jetsli.de
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.pannous.es.reindex;

import java.util.Iterator;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * Searches with the given client - used for the same cluster. Not suited for
 * other clusters as they could have an incompatible version.
 *
 * @author Peter Karich
 */
public class MySearchResponseES implements MySearchResponse {

    private SearchResponse rsp;
    private final int keepTimeInMinutes;
    private final Client client;
    private long bytes = 0;

    public MySearchResponseES(Client client, SearchResponse rsp, int keepTimeInMinutes) {
        this.client = client;
        this.rsp = rsp;
        this.keepTimeInMinutes = keepTimeInMinutes;
    }

    @Override public MySearchHits hits() {
        final SearchHits hits = rsp.getHits();
        // uh iterable is strange
        return new MySearchHits() {
            @Override public Iterable<MySearchHit> getHits() {
                return new Iterable<MySearchHit>() {
                    @Override public Iterator<MySearchHit> iterator() {
                        return new Iterator<MySearchHit>() {
                            SearchHit[] arr = hits.hits();
                            int counter = 0;

                            @Override public boolean hasNext() {
                                return counter < arr.length;
                            }

                            @Override public MySearchHit next() {
                                bytes += arr[counter].source().length;
                                MySearchHitES ret = new MySearchHitES(arr[counter]);
                                counter++;
                                return ret;
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException("Not supported yet.");
                            }
                        };
                    }
                };
            }

            @Override
            public long totalHits() {
                return hits.totalHits();
            }
        };
    }

    @Override public String scrollId() {
        return rsp.getScrollId();
    }

    @Override public int doScoll() {
        rsp = client.prepareSearchScroll(scrollId()).setScroll(TimeValue.timeValueMinutes(keepTimeInMinutes)).
                execute().actionGet();
        return rsp.getHits().hits().length;
    }

    @Override
    public long bytes() {
        return bytes;
    }

    static class MySearchHitES implements MySearchHit {

        private SearchHit sh;

        public MySearchHitES(SearchHit sh) {
            this.sh = sh;
        }

        @Override public String id() {
            return sh.id();
        }

        @Override public String parent() {
           if (sh.field("_parent") != null)
              return sh.field("_parent").value();
           return "";
        }

        @Override public long version() {
            return sh.version();
        }

        @Override public byte[] source() {
            return sh.source();
        }
    }
}
