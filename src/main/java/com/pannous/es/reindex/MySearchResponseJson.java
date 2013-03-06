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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author Peter Karich
 */
class MySearchResponseJson implements MySearchResponse {

    private int timeout = 20000;
    private HttpClient client;
    private String scrollId;
    private List<MySearchHit> bufferedHits;
    private String host;
    private int port;
    private int keepMin;
    private final boolean withVersion;
    private final long totalHits;
    private long bytes;

    public MySearchResponseJson(String searchHost, int searchPort, String searchIndexName,
            String searchType, String filter, String credentials,
            int hitsPerPage, boolean withVersion, int keepTimeInMinutes) {
        if (!searchHost.startsWith("http"))
            searchHost = "http://" + searchHost;
        this.host = searchHost;
        this.port = searchPort;
        this.withVersion = withVersion;
        keepMin = keepTimeInMinutes;
        bufferedHits = new ArrayList<MySearchHit>(hitsPerPage);
        PoolingClientConnectionManager connManager = new PoolingClientConnectionManager();
        connManager.setMaxTotal(10);
        BasicHttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParams, timeout);
        httpParams.setParameter("Authentication", "Basic " + credentials);
        client = new DefaultHttpClient(connManager, httpParams);

        // initial query to get scroll id for our specific search
        try {
            String url = searchHost + ":" + searchPort + "/" + searchIndexName + "/" + searchType
                    + "/_search?search_type=scan&scroll=" + keepMin + "m&size=" + hitsPerPage;

            String query;
            if (filter == null || filter.isEmpty())
                query = "{ \"query\" : {\"match_all\" : {}}}";
            else
                query = "{ \"filter\" : " + filter + "}";

            JSONObject res = doPost(url, query);
            scrollId = res.getString("_scroll_id");
            totalHits = res.getJSONObject("hits").getLong("total");
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override public MySearchHits hits() {
        return new MySearchHits() {
            @Override public Iterable<MySearchHit> getHits() {
                return bufferedHits;
            }

            @Override public long totalHits() {
                return totalHits;
            }
        };
    }

    @Override public String scrollId() {
        return scrollId;
    }

    @Override public int doScoll() {
        try {
            bufferedHits.clear();
            JSONObject json = doGet(host + ":" + port
                    + "/_search/scroll?scroll=" + keepMin + "m&scroll_id=" + scrollId);
            scrollId = json.getString("_scroll_id");
            JSONObject hitsJson = json.getJSONObject("hits");
            JSONArray arr = hitsJson.getJSONArray("hits");
            for (int i = 0; i < arr.length(); i++) {
                JSONObject hitJson = arr.getJSONObject(i);
                long version = -1;
                String id = hitJson.getString("_id");
                byte[] source = hitJson.getString("_source").getBytes();
                if (withVersion && hitJson.has("_version"))
                    version = hitJson.getLong("_version");
                bytes += source.length;
                MySearchHitJson res = new MySearchHitJson(id, source, version);
                bufferedHits.add(res);
            }
            return bufferedHits.size();
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long bytes() {
        return bytes;
    }

    class MySearchHitJson implements MySearchHit {

        String id;
        byte[] source;
        long version;

        public MySearchHitJson(String id, byte[] source, long version) {
            this.id = id;
            this.source = source;
            this.version = version;
        }

        @Override public String id() {
            return id;
        }

        @Override public long version() {
            return version;
        }

        @Override public byte[] source() {
            return source;
        }
    }

    protected HttpURLConnection createUrlConnection(String urlAsStr, int timeout)
            throws MalformedURLException, IOException {
        URL url = new URL(urlAsStr);
        //using proxy may increase latency
        HttpURLConnection hConn = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
        hConn.setRequestProperty("User-Agent", "ElasticSearch reindex");
        hConn.setRequestProperty("Accept", "application/json");
        hConn.setRequestProperty("content-charset", "UTF-8");
        // hConn.setRequestProperty("Cache-Control", cacheControl);
        // suggest respond to be gzipped or deflated (which is just another compression)
        // http://stackoverflow.com/q/3932117
        hConn.setRequestProperty("Accept-Encoding", "gzip, deflate");
        hConn.setConnectTimeout(timeout);
        hConn.setReadTimeout(timeout);
        return hConn;
    }

    public JSONObject doPost(String url, String content) throws JSONException {
        return new JSONObject(requestContent(new HttpPost(url), content));
    }

    public JSONObject doGet(String url) throws JSONException {
        HttpGet http = new HttpGet(url);
        try {
            http.setHeader("Content-Type", "application/json; charset=utf-8");
            HttpResponse rsp = client.execute(http);
            int ret = rsp.getStatusLine().getStatusCode();
            if (ret / 200 == 1)
                return new JSONObject(readString(rsp.getEntity().getContent(), "UTF-8"));

            throw new RuntimeException("Problem " + ret + " while " + http.getMethod()
                    + " " + readString(rsp.getEntity().getContent(), "UTF-8"));
        } catch (Exception ex) {
            throw new RuntimeException("Problem while " + http.getMethod()
                    + ", Error:" + ex.getMessage() + ", url:" + url, ex);
        } finally {
            http.releaseConnection();
        }
    }

    public String requestContent(HttpEntityEnclosingRequestBase http, String content) {
        try {
            // new UrlEncodedFormEntity
            StringEntity sendentity = new StringEntity(content, "UTF-8");
            http.setEntity(sendentity);
            http.setHeader("Content-Type", "application/json; charset=utf-8");

            HttpResponse rsp = client.execute(http);
            int ret = rsp.getStatusLine().getStatusCode();
            if (ret / 200 == 1)
                return readString(rsp.getEntity().getContent(), "UTF-8");

            throw new RuntimeException("Problem " + ret + " while " + http.getMethod()
                    + " " + readString(rsp.getEntity().getContent(), "UTF-8"));
        } catch (Exception ex) {
            throw new RuntimeException("Problem while " + http.getMethod()
                    + ", Error:" + ex.getMessage() + ", url:" + http.getURI(), ex);
        } finally {
            http.releaseConnection();
        }
    }

    public static String readString(InputStream inputStream, String encoding) throws IOException {
        InputStream in = new BufferedInputStream(inputStream);
        try {
            byte[] buffer = new byte[4096];
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            int numRead;
            while ((numRead = in.read(buffer)) != -1) {
                output.write(buffer, 0, numRead);
            }
            return output.toString(encoding);
        } finally {
            in.close();
        }
    }
}
