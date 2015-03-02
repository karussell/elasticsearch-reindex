ES=/usr/share/elasticsearch
sudo $ES/bin/plugin remove reindex
mvn -DskipTests clean package
sudo mkdir $ES/plugins/reindex
sudo cp -a ./target/classes/com $ES/plugins/reindex
sudo cp -a ./target/classes/es-plugin.properties $ES/plugins/reindex
sudo service elasticsearch restart
