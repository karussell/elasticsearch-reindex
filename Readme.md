# Install

> mvn -DskipTests clean package

> sudo $ES_HOME/bin/plugin install karussell/elasticsearch-reindex

> // or the local one: sudo $ES_HOME/bin/plugin -url ./target -install reindex

> sudo service elasticsearch restart

you should see 'loaded [reindex], sites []' in the logs

# Deinstallation

> sudo $ES_HOME/bin/plugin remove reindex

# Usage

> curl -XPUT 'http://localhost:9200/indexold/typeold/_reindex?newIndex=indexnew&newType=typenew' -d '
  { "term" : { "count" : 2 }
  }
'

This refeeds all documents in index 'indexold' with type 'typeold' into the index 'indexnew' with type 'typenew'.
But only documents matching the specified term query will be refeeded.

Hints:
 * the index 'indexnew' and the type 'typenew' should exist. If not, the defaults of elasticsearch will apply
 * the parameters 'newIndex' and 'newType' are optional and the old one will be used if not provided
 * the query is also optional. match_all will be used if not provided
 * This command can be used to update documents of an index, e.g. after you change the settings of a type
 * This command can be used to change the index settings like shard count

