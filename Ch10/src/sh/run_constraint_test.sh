ACCUMULO_LIB=/opt/cloud/accumulo-1.4.1/lib/*
HADOOP_LIB=/Applications/hadoop-0.20.2-cdh3u1/*:/Applications/hadoop-0.20.2-cdh3u1/lib/*
ZOOKEEPER_LIB=/opt/cloud/zookeeper-3.4.2/*
java -cp $ACCUMULO_LIB:$HADOOP_LIB:$ZOOKEEPER_LIB:accumulo-examples.jar examples.accumulo.DtgConstraintMain\
 00993877573819_9223370801921575807\
 2012-08-07\
 test\
 root\
 password\
 localhost:2181