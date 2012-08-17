package examples.accumulo;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class SecurityScanMain  {

    public static final long MAX_MEMORY= 10000L;
    public static final long MAX_LATENCY=1000L;
    public static final int MAX_WRITE_THREADS = 4;
    public static final String TEST_TABLE = "acled";
    public static final Text COLUMN_FAMILY = new Text("cf");
    public static final Text THREAT_QUAL = new Text("trt_lvl");


    public static void main(String[] args)throws Exception{
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
        Authorizations allowedAuths = connector.securityOperations().getUserAuthorizations(user);
        BatchWriter writer = connector.createBatchWriter(TEST_TABLE, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS);
        Mutation m1 = new Mutation(new Text("eventA"));
        m1.put(COLUMN_FAMILY,
                THREAT_QUAL,
                new ColumnVisibility("(p1|p2|p3)"),
                new Value("moderate".getBytes()));
        Mutation m2 = new Mutation(new Text("eventB"));
        m2.put(COLUMN_FAMILY,
                THREAT_QUAL,
                new ColumnVisibility("(p4|p5)"),
                new Value("severe".getBytes()));
        writer.addMutation(m1);
        writer.addMutation(m2);
        writer.close();
        Scanner scanner = connector.createScanner(TEST_TABLE, allowedAuths);
        scanner.fetchColumn(COLUMN_FAMILY, THREAT_QUAL);
        boolean found = false;
        for(Map.Entry<Key, Value> item: scanner){
            System.out.println("Scan found: " + item.getKey().getRow().toString() + " threat level: " + item.getValue().toString());
            found = true;
        }
        if(!found)
            System.out.println("no threat levels are visible with your current user auths: " + allowedAuths.serialize());
    }
}
