package examples.accumulo;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

public class SourceFilterMain {

    public static final String TEST_TABLE = "acled";

    public static final Text COLUMN_FAMILY = new Text("cf");
    public static final Text SRC_QUAL = new Text("src");

    public static void main(String[] args) throws Exception {
        if(args.length < 5) {
            System.err.println("usage: <src> <instance name>  <user> <password> <zookeepers>");
            System.exit(0);
        }
        String src = args[0];
        String instanceName = args[1];
        String user = args[2];
        String pass = args[3];
        String zooQuorum = args[4];
        ZooKeeperInstance ins = new ZooKeeperInstance(instanceName, zooQuorum);
        Connector connector = ins.getConnector(user, pass);
        Scanner scan = connector.createScanner(TEST_TABLE, new Authorizations());
        scan.fetchColumn(COLUMN_FAMILY, SRC_QUAL);
        IteratorSetting iter = new IteratorSetting(15, "regexfilter", RegExFilter.class);
        iter.addOption(RegExFilter.VALUE_REGEX, src);
        scan.addScanIterator(iter);
        int count = 0;
        for(Map.Entry<Key, Value> row : scan) {
          System.out.println("row: " + row.getKey().getRow().toString());
          count++;
        }
        System.out.println("total rows: " + count);
    }
}
