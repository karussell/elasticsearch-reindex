ES=/usr/share/elasticsearch
sudo $ES/bin/plugin remove reindex
mvn -DskipTests clean package
sudo $ES/bin/plugin -url ./target -install reindex
sudo service elasticsearch restart