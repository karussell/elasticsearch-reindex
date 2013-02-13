ES=/usr/share/elasticsearch
sudo $ES/bin/plugin remove reindex
mvn -DskipTests clean package
FILE=`ls ./target/elasticsearch-*zip`
sudo $ES/bin/plugin -url file:$FILE -install reindex
sudo service elasticsearch restart