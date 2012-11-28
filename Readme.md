# Install

> mvn -DskipTests clean package

> sudo $ES_HOME/bin/plugin install karussell/elasticsearch-reindex

> // or the local one: sudo $ES_HOME/bin/plugin -url ./target -install reindex

> sudo service elasticsearch restart

you should see 'loaded [reindex], sites []' in the logs

# Deinstallation

> sudo $ES_HOME/bin/plugin remove reindex

# Usage

## WARNINGs / TODOs:
 * please try on your local machine before using it in production - especially the case searchHost!=localhost could be problematic for your performance/IO
 * the call is not async and not stopable (except you stop the requested server) => The plugin should probably be better a river
 * if you have two servers on localhost and the queried server port is 9201 and you want to search
   the different server at 9200. => you have to use: searchHost=127.0.0.1&searchPort=9200

## Same cluster 

The internal Java API will be used:

> curl -XPUT 'http://localhost:9200/indexold/typeold/_reindex?newIndex=indexnew&newType=typenew' -d '
>  { "term" : { "count" : 2 } }'

This refeeds all documents in index 'indexold' with type 'typeold' into the index 'indexnew' with type 'typenew'.
But only documents matching the specified filter will be refeeded.

## Different cluster 

Now JSONObjects and the HttpClient will be used. TODO that is probably not efficient in terms of RAM/CPU?!:

> curl -XPUT 'http://localhost:9200/indexold/typeold/_reindex?newIndex=indexnew&newType=typenew&searchHost=yourElasticsearchHost.com&searchPort=9200' -d '
>  { "term" : { "count" : 2 } }'

Further parameters:
 * hitsPerPage - used as search limit and at the same time for bulk indexing (default 100)
 * keepTimeInMinutes - the maximum time in minutes a scroll search is valid (default 30) increase if you have more data
 * withVersion - if the version of a document should be respected (default false)
 * waitInSeconds - pause the specified time after every request pair (one search+one bulkIndex). 
   This avoids heavy load on the search or on the indexing server/cluster. This way it is very easy
   e.g. to grab even a massive amount of data from your production servers into your local machine.

Hints:
 * the index 'indexnew' and the type 'typenew' should exist. E.g. it is important that you create the index with the appropriated config! If not, the defaults of elasticsearch will apply
 * the parameters 'newIndex' and 'newType' are optional and the old one will be used if not provided
 * the filter is also optional
 * This command can be used to update documents of an index, e.g. after you change the settings of a type
 * This command can be used to change the index settings like shard count