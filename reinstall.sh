ES=/usr/share/elasticsearch
sudo $ES/bin/plugin remove rollindex
mvn -DskipTests clean package
sudo $ES/bin/plugin -url ./target -install rollindex
sudo service elasticsearch restart