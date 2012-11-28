# Install

> mvn -DskipTests clean package

> sudo $ES_HOME/bin/plugin install karussell/elasticsearch-reindex

> // or the local one: sudo $ES_HOME/bin/plugin -url ./target -install reindex

> sudo service elasticsearch restart

you should see 'loaded [reindex], sites []' in the logs

# Deinstallation

> sudo $ES_HOME/bin/plugin remove reindex

# Usage

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

Hints:
 * the index 'indexnew' and the type 'typenew' should exist. If not, the defaults of elasticsearch will apply
 * the parameters 'newIndex' and 'newType' are optional and the old one will be used if not provided
 * the filter is also optional
 * This command can be used to update documents of an index, e.g. after you change the settings of a type
 * This command can be used to change the index settings like shard count

