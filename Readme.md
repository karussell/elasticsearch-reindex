# Install

> mvn -DskipTests clean package

> sudo $ES_HOME/bin/plugin -url ./target -install rollindex

> sudo service elasticsearch restart

you should see 'loaded [rollindex], sites []' in the logs

# Deinstallation

> sudo $ES_HOME/bin/plugin remove rollindex

# Usage

The following command:
> curl -XPUT 'http://localhost:9200/_rollindex?index=test&searchIndices=2&rollIndices=3'

creates a new index with 3 aliases: 
 * 'test_feed' which acts as a feeding alias
 * 'test_search' which spans over the last 2 indices
 * 'test_roll' which spans over the last 3 indices, all older indices will be closed

Call the command several times and you get a feeling of what it does.
Then create a cron job which calls it with the periodic you like. But of course you can even
call it manually in aperiodic cycles.

See the source code to change index creation settings or if you want to delete the indices after
leaving the roll alias

# FAQ

 * Why do I'm getting IndexAlreadyExistsException? You roll too often, reduce to per minute at maximum. 
   Or change the pattern to include the seconds.
 * Why is no scheduling including? To keep it simple. Otherwise we would require quartz or similar
 