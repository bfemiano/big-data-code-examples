GIRAPH_PATH=lib/giraph/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar
HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$GIRAPH_PATH
JAR_PATH=dist/employee_examples.jar
export HADOOP_CLASSPATH
hadoop jar $JAR_PATH emp_breadth_search -libjars $GIRAPH_PATH,$JAR_PATH /input/gooftech /output/gooftech 'Valery Dorado' 'Gertha Linda' localhost:2181
