package examples.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.List;

public class DtgConstraintMain {

    public static final long MAX_MEMORY= 10000L;
    public static final long MAX_LATENCY=1000L;
    public static final int MAX_WRITE_THREADS = 4;
    public static final String TEST_TABLE = "acled";
    public static final Text COLUMN_FAMILY = new Text("cf");
    public static final Text DTG_QUAL = new Text("dtg");

    public static void main(String[] args) throws Exception{
        if(args.length < 6){
            System.err.println("examples.accumulo.DtgConstraintMain <row_id> <dtg> <instance_name> <user> <password> <zookeepers>");
            System.exit(0);
        }
        String rowID = args[0];
        byte[] dtg = args[1].getBytes();
        String instanceName = args[2];
        String user = args[3];
        String pass = args[4];
        String zooQuorum = args[5];
        ZooKeeperInstance ins;
        Connector connector = null;
        BatchWriter writer = null;
        try {

            ins = new ZooKeeperInstance(instanceName, zooQuorum);
            connector = ins.getConnector(user, pass);
            writer = connector.createBatchWriter(TEST_TABLE, MAX_MEMORY, MAX_LATENCY, MAX_WRITE_THREADS);
            connector.tableOperations().setProperty(TEST_TABLE,
                 Property.TABLE_CONSTRAINT_PREFIX.getKey() + 1,
                   DtgConstraint.class.getName());
            Mutation validMutation = new Mutation(new Text(rowID));
            validMutation.put(COLUMN_FAMILY, DTG_QUAL, new Value(dtg));
            writer.addMutation(validMutation);
            writer.close();
        } catch (MutationsRejectedException e) {
            List<ConstraintViolationSummary> summaries = e.getConstraintViolationSummaries();
            for(ConstraintViolationSummary sum : summaries) {
                System.err.println(sum.toString());
            }
        }
    }
}
