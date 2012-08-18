package examples.accumulo;


import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Override;
import java.util.HashSet;

public class SourceCountJob extends Configured implements Tool {


    private Configuration conf;
    private static final Text FAMILY = new Text("cf");
    private static final Text SOURCE = new Text("src");

    public SourceCountJob(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(args.length < 6) {
            System.err.println(printUsage());
            System.exit(0);
        }

        String tableName = args[0];
        String outputStr = args[1];
        String instanceName = args[2];
        String user = args[3];
        String pass = args[4];
        String zooQuorum = args[5];
        AccumuloInputFormat.setInputInfo(conf, user, pass.getBytes(), tableName, new Authorizations());
        AccumuloInputFormat.setZooKeeperInstance(conf, instanceName, zooQuorum);
        HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
        columnsToFetch.add(new Pair<Text, Text>(FAMILY, SOURCE));
        AccumuloInputFormat.fetchColumns(conf, columnsToFetch);

        Job job = new Job(conf, "Count distinct sources in ACLED");
        job.setInputFormatClass(AccumuloInputFormat.class);
        job.setMapperClass(ACLEDSourceMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ACLEDSourceReducer.class);
        job.setCombinerClass(ACLEDSourceReducer.class);
        job.setJarByClass(getClass());
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, clearOutputDir(outputStr));
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true) ? 0 : 1;


    }

    private String printUsage() {
        return "<tablename> <output> <instance_name> <username> <password> <zoohosts>";
    }

    private Path clearOutputDir(String outputStr)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputStr);
        fs.delete(path, true);
        return path;
    }

    public static class ACLEDSourceMapper
            extends Mapper<Key, Value, Text, IntWritable> {

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable(1);

        @Override
        protected void map(Key key, Value value,
                           Context context) throws IOException, InterruptedException {

            outKey.set(value.get());
            context.write(outKey, outValue);
        }
    }

    public static class ACLEDSourceReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

          int count = 0;
          for(IntWritable value : values) {
              count += value.get();
          }
          outValue.set(count);
          context.write(key, outValue);
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
        ToolRunner.run(new SourceCountJob(conf), args);
    }
}
