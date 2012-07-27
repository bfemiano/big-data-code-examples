package main.java.employee;

import org.apache.giraph.graph.*;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Start with specified employee, mark the target if message is received
 */
public class EmployeeBreadthFirstSearch implements Tool{

    public static final String NAME = "emp_breadth_search";

    private Configuration conf;
    private static final String SOURCE_ID = "emp_src_id";
    private static final String DEST_ID = "emp_dest_id";

    public EmployeeBreadthFirstSearch(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length < 5) {
            System.err.println(printUsage());
            System.exit(1);
        }
        if(args.length > 5) {
            System.err.println("too many arguments. " +
                    "Did you forget to quote the source or destination ID name ('firstname lastname')");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        String source_id = args[2];
        String dest_id = args[3];
        String zooQuorum = args[4];

        conf.set(SOURCE_ID, source_id);
        conf.set(DEST_ID, dest_id);
        conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
        conf.setBoolean(GiraphJob.USE_SUPERSTEP_COUNTERS, false);
        conf.setInt(GiraphJob.CHECKPOINT_FREQUENCY, 0);
        GiraphJob job = new GiraphJob(conf, "determine connectivity between " + source_id + " and " + dest_id);
        job.setVertexClass(EmployeeSearchVertex.class);
        job.setVertexInputFormatClass(EmployeeRDFTextInputFormat.class);
        job.setVertexOutputFormatClass(BreadthFirstTextOutputFormat.class);
        job.setZooKeeperConfiguration(zooQuorum);

        FileInputFormat.addInputPath(job.getInternalJob(), new Path(input));
        FileOutputFormat.setOutputPath(job.getInternalJob(), removeAndSetOutput(output));

        job.setWorkerConfiguration(1, 1, 100.0f);   //pseudo distributed testing

        if(job.run(true)) {
            long srcCounter = job.getInternalJob().getCounters().
                    getGroup("Search").findCounter("Source Id found").getValue();
            long dstCounter = job.getInternalJob().getCounters().
                    getGroup("Search").findCounter("Dest Id found").getValue();
            if(srcCounter == 0 || dstCounter == 0) {
                 System.out.println("Source and/or Dest Id not found in dataset. Check your arguments.");
            }
            return 0;
        } else {
            return 1;
        }
    }

    private Path removeAndSetOutput(String outputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputDir);
        fs.delete(path, true);
        return path;
    }

    private String printUsage() {
        return "usage: <input> <output> <single quoted source_id> <single quoted dest_id> <zookeeper_quorum>";
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
        System.exit(ToolRunner.run(new EmployeeBreadthFirstSearch(new Configuration()), args));
    }


    public static class EmployeeSearchVertex<I extends WritableComparable,
            V extends Writable, E extends Writable, M extends Writable>
            extends EdgeListVertex<Text, IntWritable, NullWritable, IntWritable> {

        private IntWritable msg = new IntWritable(1);

        private boolean isSource() {
            return getId().toString().equals(
                    getConf().get(SOURCE_ID));
        }

        private boolean isDest() {
            return getId().toString().equals(
                    getConf().get(DEST_ID));
        }

        @Override
        public void compute(Iterable<IntWritable> messages) throws IOException {
            if(getSuperstep() == 0) {
                if(isSource()) {
                    getContext().getCounter("Search", "Source Id found").increment(1l);
                    sendMessageToAllEdges(msg);
                }  else if(isDest()){
                    getContext().getCounter("Search", "Dest Id found").increment(1l);
                }
            }
            boolean connectedToSourceId = false;
            for(IntWritable msg : messages) {
                if(isDest()) {
                    System.out.println("DestId got message");
                    setValue(msg);
                }
                connectedToSourceId = true;
            }
            if(connectedToSourceId)
                sendMessageToAllEdges(msg);
            voteToHalt();
        }
    }

    public static class BreadthFirstTextOutputFormat extends
            TextVertexOutputFormat<Text, IntWritable, NullWritable> {
        /**
         * Simple text based vertex writer
         */
        private static class EmployeeRDFVertexWriter
                extends TextVertexWriter<Text, IntWritable, NullWritable> {
            /**
             * Initialize with the LineRecordWriter.
             *
             * @param lineRecordWriter Line record writer from TextOutputFormat
             */
            private Text valOut = new Text();
            private String sourceId = null;
            private String destId = null;

            public EmployeeRDFVertexWriter(
                    String sourceId, String destId, RecordWriter<Text, Text> lineRecordWriter) {
                super(lineRecordWriter);
                this.sourceId = sourceId;
                this.destId = destId;
            }

            @Override
            public void writeVertex(
                    Vertex<Text, IntWritable, NullWritable, ?> vertex)
                    throws IOException, InterruptedException {

                if(vertex.getId().toString().equals(destId)) {
                    if(vertex.getValue().get() > 0) {
                        getRecordWriter().write(new Text(sourceId + " is connected to " + destId), new Text(""));
                    } else {
                        getRecordWriter().write(new Text(sourceId + " is not connected to " + destId), new Text(""));
                    }
                }
            }
        }

        @Override
        public VertexWriter<Text, IntWritable, NullWritable>
        createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                    textOutputFormat.getRecordWriter(context);
            String sourceId = context.getConfiguration().get(SOURCE_ID);
            String destId = context.getConfiguration().get(DEST_ID);
            return new EmployeeRDFVertexWriter(sourceId, destId, recordWriter);
        }
    }

}
