# Install

mvn clean package
sudo $ES_HOME/bin/plugin -url ./target -install rollindex

sudo service elasticsearch restart

you should see 'loaded [rollindex], sites []' in the logs

# Deinstallation

sudo $ES_HOME/bin/plugin remove rollindex

# Usage

 * call this line 5 times and you get a feeling what it does:
   curl -XPUT 'http://localhost:9200/_rollindex?index=test&searchIndices=2&rollIndices=3'
 * Then create a cron job which calls with the periodic you like:
 * see source code for more parameters for the new index name and settings