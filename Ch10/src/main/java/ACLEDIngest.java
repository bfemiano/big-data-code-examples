package examples.accumulo;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;

public class ACLEDIngest extends Configured implements Tool {


    private Configuration conf;

    public ACLEDIngest(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 8) {
            System.err.println(printUsage());
            System.exit(0);
        }

        Job job = new Job(conf, "ACLED ingest to Accumulo");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ACLEDIngestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ACLEDIngestReducer.class);
        job.setPartitionerClass(RangePartitioner.class);
        job.setJarByClass(getClass());

        String input = args[0];
        String outputStr = args[1];
        String instanceName = args[2];
        String tableName = args[3];
        String user = args[4];
        String pass = args[5];
        String zooQuorum = args[6];
        String localSplitFile = args[7];

        FileInputFormat.addInputPath(job, new Path(input));
        AccumuloFileOutputFormat.setOutputPath(job, clearOutputDir(outputStr));
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);

        AccumuloTableAssistant tableAssistant = new AccumuloTableAssistant.Builder().
                setInstanceName(instanceName).setTableName(tableName).setUser(user)
                .setPassword(pass).setZooQuorum(zooQuorum).build();

        String splitFileInHDFS = "/tmp/splits.txt";
        int numSplits = 0;
        tableAssistant.createTableIfNotExists();
        if(localSplitFile != null) {
            numSplits = tableAssistant.presplitAndWriteHDFSFile(conf, localSplitFile, splitFileInHDFS);
        }
        RangePartitioner.setSplitFile(job, splitFileInHDFS);
        job.setNumReduceTasks(numSplits + 1);

        if(job.waitForCompletion(true)) {
            tableAssistant.loadImportDirectory(conf, outputStr);
        }
        return 0;
    }

    private String printUsage() {
        return "<input> <output> <instance_name> <tablename> " +
                "<username> <password> <zoohosts> <splits_file_path>";
    }

    private Path clearOutputDir(String outputStr)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputStr);
        fs.delete(path, true);
        return path;
    }

    public static class ACLEDIngestMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text outKey = new Text();
        private static final Pattern tabPattern = Pattern.compile("[\\t]");
        private ACLEDRowIDGenerator gen = new ACLEDRowIDGenerator();

        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            String[] values = tabPattern.split(value.toString());
            if(values.length == 8)  {
                String [] rowKeyFields = new String[] {values[4], values[5], values[1]}; //lat,lon,timestamp
                outKey.set(gen.getRowID(rowKeyFields));
                context.write(outKey, value);
            } else {
                context.getCounter("ACLED Ingest", "malformed records").increment(1l);
            }
        }
    }

    public static class ACLEDIngestReducer
            extends Reducer<Text, Text, Key, Value> {

        private Key outKey;
        private Value outValue = new Value();
        private Text cf = new Text("cf");
        private Text qual = new Text();
        private static final Pattern tabPattern = Pattern.compile("[\\t]");

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {

            int found = 0;
            for(Text value : values) {
                String[] cells = tabPattern.split(value.toString());
                if(cells.length == 8) {
                    if(found < 1) { //don't write duplicates
                        write(context,  key , cells[3], "atr");
                        write(context,  key , cells[1], "dtg");
                        write(context,  key , cells[7], "fat");
                        write(context,  key , cells[4], "lat");
                        write(context,  key , cells[0], "loc"); //sorted order for rc files
                        write(context,  key , cells[5], "lon");
                        write(context,  key , cells[6], "src");
                        write(context,  key , cells[2], "type");
                    } else {
                       context.getCounter("ACLED Ingest", "duplicates").increment(1l);
                    }
                } else {
                    context.getCounter("ACLED Ingest", "malformed records missing a field").increment(1l);
                }
                found++;
            }
        }

        private void write(Context context, Text key, String cell, String qualStr)
                throws IOException, InterruptedException {
            if(!cell.toUpperCase().equals("NULL")) {
                qual.set(qualStr);
                outKey = new Key(key, cf, qual, System.currentTimeMillis());
                outValue.set(cell.getBytes());
                context.write(outKey, outValue);
            }
        }

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
        Configuration conf = CachedConfiguration.getInstance();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        ToolRunner.run(new ACLEDIngest(conf), args);
    }
}
