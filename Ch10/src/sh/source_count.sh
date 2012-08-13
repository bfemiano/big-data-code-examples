tool.sh accumulo_examples.jar examples.accumulo.SourceCountJob\
 -Dmapred.reduce.tasks=4\
 acled\
 /output/accumulo_source_count/\
 test\
 root\
 password\
 localhost:2181
hadoop fs -cat /output/accumulo_source_count/part* > source_count.txt