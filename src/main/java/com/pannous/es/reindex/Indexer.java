package com.pannous.es.reindex;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Indexer implements Runnable {

    private final ESLogger logger;
    private Client client;
    private MySearchResponse rsp;
    private String newIndex;
    private String newType;
    private boolean withVersion;
    private float waitSeconds;


    public Indexer(Client client, MySearchResponse rsp, String newIndex, String newType, boolean withVersion, float waitSeconds) {

        logger = Loggers.getLogger(this.getClass());

        this.client = client;
        this.rsp = rsp;
        this.newIndex = newIndex;
        this.newType = newType;
        this.withVersion = withVersion;
        this.waitSeconds = waitSeconds;
    }

    @Override
    public void run() {

        boolean flushEnabled = false;
        long total = rsp.hits().totalHits();
        int collectedResults = 0;
        int failed = 0;
        while (true) {
            if (collectedResults > 0 && waitSeconds > 0) {
                try {
                    Thread.sleep(Math.round(waitSeconds * 1000));
                } catch (InterruptedException ex) {
                    break;
                }
            }
            StopWatch queryWatch = new StopWatch().start();
            int currentResults = rsp.doScoll();
            if (currentResults == 0)
                break;

            MySearchHits res = rsp.hits();
            if (res == null)
                break;
            queryWatch.stop();
            StopWatch updateWatch = new StopWatch().start();
            failed += bulkUpdate(res, newIndex, newType, withVersion).size();
            if (flushEnabled)
                client.admin().indices().flush(new FlushRequest(newIndex)).actionGet();

            updateWatch.stop();
            collectedResults += currentResults;
            logger.debug("Progress " + collectedResults + "/" + total
                    + ". Time of update:" + updateWatch.totalTime().getSeconds() + " query:"
                    + queryWatch.totalTime().getSeconds() + " failed:" + failed);
        }
        String str = "found " + total + ", collected:" + collectedResults
                + ", transfered:" + (float) rsp.bytes() / (1 << 20) + "MB";
        if (failed > 0)
            logger.warn(failed + " FAILED documents! " + str);
        else
            logger.info(str);
    }

    Collection<Integer> bulkUpdate(MySearchHits objects, String indexName,
                                   String newType, boolean withVersion) {
        BulkRequestBuilder brb = client.prepareBulk();
        for (MySearchHit hit : objects.getHits()) {
            if (hit.id() == null || hit.id().isEmpty()) {
                logger.warn("Skipped object without id when bulkUpdate:" + hit);
                continue;
            }

            try {
                IndexRequest indexReq = Requests.indexRequest(indexName).type(newType).id(hit.id()).source(hit.source());
                if (withVersion)
                    indexReq.version(hit.version());

                brb.add(indexReq);
            } catch (Exception ex) {
                logger.warn("Cannot add object:" + hit + " to bulkIndexing action." + ex.getMessage());
            }
        }
        if (brb.numberOfActions() > 0) {
            BulkResponse rsp = brb.execute().actionGet();
            if (rsp.hasFailures()) {
                List<Integer> list = new ArrayList<Integer>(rsp.items().length);
                for (BulkItemResponse br : rsp.items()) {
                    if (br.isFailed())
                        list.add(br.itemId());
                }
                return list;
            }
        }
        return Collections.emptyList();
    }

}
