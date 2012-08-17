package examples.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.*;

public class TotalFatalityCombinerMain {

    public static final long MAX_MEMORY= 10000L;
    public static final long MAX_LATENCY=1000L;
    public static final int MAX_WRITE_THREADS = 4;
    public static final String TEST_TABLE = "acled";
    public static final Text COLUMN_FAMILY = new Text("cf");
    public static final Text FATALITIES_QUAL = new Text("fat");

    public static void main(String[] args) throws Exception {
        if(args.length < 4) {
            System.err.println("usage: <instance name>  <user> <password> <zookeepers>");
            System.exit(0);
        }
        String instanceName = args[0];
        String user = args[1];
        String pass = args[2];
        String zooQuorum = args[3];
        ZooKeeperInstance ins = new ZooKeeperInstance(instanceName, zooQuorum);
        Connector connector = ins.getConnector(user, pass);
        if(!connector.tableOperations().exists(TEST_TABLE))
           connector.tableOperations().create(TEST_TABLE);
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS);
        Mutation m1 = new Mutation("eventA");
        m1.put(COLUMN_FAMILY, FATALITIES_QUAL, new Value("10".getBytes()));

        Mutation m2 = new Mutation("eventA");
        m2.put(COLUMN_FAMILY, FATALITIES_QUAL, new Value("5".getBytes()));

        Mutation m3 = new Mutation("eventB");
        m3.put(COLUMN_FAMILY, FATALITIES_QUAL, new Value("7".getBytes()));

        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.addMutation(m3);
        writer.close();

        IteratorSetting iter = new IteratorSetting(1, SummingCombiner.class);
        LongCombiner.setEncodingType(iter, SummingCombiner.Type.STRING);
        Combiner.setColumns(iter, Collections.singletonList(new IteratorSetting.Column(COLUMN_FAMILY, FATALITIES_QUAL)));
        Scanner scan = connector.createScanner(TEST_TABLE, new Authorizations());
        scan.setRange(new Range(new Text("eventA"), new Text("eventB")));
        scan.fetchColumn(COLUMN_FAMILY, FATALITIES_QUAL);
        scan.addScanIterator(iter);
        for(Map.Entry<Key, Value> item : scan){
            System.out.print(item.getKey().getRow().toString() + ": fatalities: ");
            System.out.println(new String(item.getValue().get()));
        }
    }
}

