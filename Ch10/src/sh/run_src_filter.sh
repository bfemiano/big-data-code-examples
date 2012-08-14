ACCUMULO_LIB=/opt/cloud/accumulo-1.4.1/lib/*
HADOOP_LIB=/Applications/hadoop-0.20.2-cdh3u1/*:/Applications/hadoop-0.20.2-cdh3u1/lib/*
ZOOKEEPER_LIB=/opt/cloud/zookeeper-3.4.2/*
java -cp $ACCUMULO_LIB:$HADOOP_LIB:$ZOOKEEPER_LIB:accumulo-examples.jar examples.accumulo.SourceFilterMain\
 'Panafrican News Agency'\
 test\
 root\
 password\
 localhost:2181
