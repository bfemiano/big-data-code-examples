package main.java.employee;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Value based on number of hops. vertices receiving incoming messages increment the message
 */
public class EmployeeShortestPath implements Tool{

    public static final String NAME = "emp_shortest_path";

    private Configuration conf;
    private static final String SOURCE_ID = "emp_source_id";

    public EmployeeShortestPath(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 4) {
            System.err.println(printUsage());
            System.exit(1);
        }
        if(args.length > 4) {
            System.err.println("too many arguments. " +
                    "Did you forget to quote the source ID name ('firstname lastname')");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        String source_id = args[2];
        String zooQuorum = args[3];

        conf.set(SOURCE_ID, source_id);
        conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
        conf.setBoolean(GiraphJob.USE_SUPERSTEP_COUNTERS, false);
        conf.setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
        GiraphJob job = new GiraphJob(conf, "single-source shortest path for employee: " + source_id);
        job.setVertexClass(EmployeeShortestPathVertex.class);
        job.setVertexInputFormatClass(EmployeeRDFTextInputFormat.class);
        job.setVertexOutputFormatClass(EmployeeRDFTextOutputFormat.class);
        job.setZooKeeperConfiguration(zooQuorum);

        FileInputFormat.addInputPath(job.getInternalJob(), new Path(input));
        FileOutputFormat.setOutputPath(job.getInternalJob(), removeAndSetOutput(output));

        job.setWorkerConfiguration(1, 1, 100.0f);   //pseudo distributed testing
        return job.run(true) ? 0 : 1;
    }

    private Path removeAndSetOutput(String outputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputDir);
        fs.delete(path, true);
        return path;
    }

    private String printUsage() {
        return "usage: <input> <output> <single quoted source_id> <zookeeper_quorum>";
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new EmployeeShortestPath(new Configuration()), args));
    }


    public static class EmployeeShortestPathVertex<I extends WritableComparable,
            V extends Writable, E extends Writable, M extends Writable>
            extends EdgeListVertex<Text, IntWritable, NullWritable, IntWritable> {

        private IntWritable max = new IntWritable(Integer.MAX_VALUE);
        private IntWritable msg = new IntWritable(1);

        private boolean isSource() {
            return getId().toString().equals(
                    getConf().get(SOURCE_ID));
        }

        @Override
        public void compute(Iterable<IntWritable> messages) throws IOException {
            if(getSuperstep() == 0) {
                setValue(max);
                if(isSource()) {
                    for(Edge<Text, NullWritable> e : getEdges()) {
                        sendMessage(e.getTargetVertexId(), msg);
                    }
                }
            }
            int min = getValue().get();
            for(IntWritable msg : messages) {
                min = Math.min(msg.get(), min);
            }
            if(min < getValue().get()) {
                setValue(new IntWritable(min));
                msg.set(min + 1);
                sendMessageToAllEdges(msg);
            }
            voteToHalt();
        }
    }
}
