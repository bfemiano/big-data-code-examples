package mil.rebel.taint.accumulo;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
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
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class DelimitedTextIngester extends Configured implements Tool{

    public static final String INPUT_ARG = "in";
    public static final String INSTANCE_NAME = "ins";
    public static final String OUTPUT_ARG = "out";
    public static final String TABLE_ARG = "table";
    public static final String USER_ARG = "u";
    public static final String PASSWORD_ARG = "p";
    public static final String ZOO_ARG = "zoo";
    public static final String COLUMN_FAMILY_ARG = "cf";
    public static final String COLUMN_ARG = "cols";
    public static final String SPLIT_FILE_PROP = "splits";
    public static final String SEPARATOR_CONF_KEY = "separator";
    public static final String NAME = "bulk_ingest";

    private final Logger logger = Logger.getLogger(DelimitedTextIngester.class);
    private static final String DEFAULT_SEPARATOR = "\t";
    private Configuration conf;

    private static Map<String, String> requiredOptions = new HashMap<String, String>();


    static {
        requiredOptions.put(INPUT_ARG, "input from HDFS");
        requiredOptions.put(OUTPUT_ARG, "temp output directory in HDFS to place accumulo files");
        requiredOptions.put(TABLE_ARG, "Accumulo table to load files into");
        requiredOptions.put(INSTANCE_NAME, "instance name in zookeeper");
        requiredOptions.put(USER_ARG, "username");
        requiredOptions.put(PASSWORD_ARG, "password");
        requiredOptions.put(ZOO_ARG, "comma separated zookeper quorum");
        requiredOptions.put(COLUMN_FAMILY_ARG, "single column family to handle all qualifiers in 'cols' argument");
        requiredOptions.put(COLUMN_ARG, "comma separated qual:cell_vis pairs " +
                "to handle line tokens. cell_vis is optional. (example: field1,field2:vis");

    }

    public DelimitedTextIngester(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {

        CommandLine cmd = loadCommandOptions(args);
        conf.set(COLUMN_ARG, cmd.getOptionValue(COLUMN_ARG));
        conf.set(SEPARATOR_CONF_KEY,
                (cmd.hasOption(SEPARATOR_CONF_KEY) ?
                        cmd.getOptionValue(SEPARATOR_CONF_KEY) : DEFAULT_SEPARATOR));

        String localSplitFile = null;
        if(cmd.hasOption(SPLIT_FILE_PROP)) {
            localSplitFile = cmd.getOptionValue(SPLIT_FILE_PROP);
        }

        Job job = new Job(conf, "TSV ingest to Accumulo");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LineToAccumuloTupleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CellValues.class);
        job.setReducerClass(IngestKeyValueReducer.class);
        job.setPartitionerClass(RangePartitioner.class);

        String input = cmd.getOptionValue(INPUT_ARG);
        String outputStr = cmd.getOptionValue(OUTPUT_ARG);
        String user = cmd.getOptionValue(USER_ARG);
        String pass = cmd.getOptionValue(PASSWORD_ARG);
        String tableName = cmd.getOptionValue(TABLE_ARG);
        String instanceName = cmd.getOptionValue(INSTANCE_NAME);
        String zooQuorum = cmd.getOptionValue(ZOO_ARG);

        FileInputFormat.addInputPath(job, new Path(input));

        AccumuloFileOutputFormat.setOutputPath(job, clearOutputDir(outputStr));
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);

        AccumuloTableAssistant tableAssistant = new AccumuloTableAssistant.Builder().
                setInstanceName(instanceName).setTableName(tableName).setUser(user)
                .setPassword(pass).setZooQuorum(zooQuorum).build();

        String splitFileInHDFS = outputStr + "/splits.txt";
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

    private Path clearOutputDir(String outputStr)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputStr);
        fs.delete(path, true);
        return path;
    }

    public CommandLine loadCommandOptions(String[] args)
            throws ParseException{


        Options options = new Options();
        for(Map.Entry<String, String> pair: requiredOptions.entrySet()) {
            options.addOption(pair.getKey(), true, pair.getValue());

        }

        options.addOption(SEPARATOR_CONF_KEY, true, "<optional> separator token, defaults to tab-delimited");
        options.addOption(SPLIT_FILE_PROP, true, "<optional> split file path on local disk");

        HelpFormatter formatter = new HelpFormatter();
        if(args.length == 0) {
            formatter.printHelp( "build_ingest", options );
            System.exit(0);
        }

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args, false);
        for(Map.Entry<String, String> arg: requiredOptions.entrySet()) {
            if (!cmd.hasOption(arg.getKey())) {
                logger.error("missing required argument: " + arg.getKey());
                formatter.printHelp( "build_ingest", options );
                throw new IllegalArgumentException("missing required argument " + arg.getKey());
            }
        }
        return cmd;
    }

    public static class LineToAccumuloTupleMapper
            extends Mapper<LongWritable, Text, Text, CellValues>  {

        private IngestLineParser lineParser;
        private Text outKey = new Text();
        private CellValues outVal;

        @Override
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            lineParser = new IngestLineParser(context.getConfiguration());

        }

        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            //outKey.set(lineParser.getKey(key, value));
            //CellValues[] = lineParser.parse(key, value);
            //TODO: Add argument for row_key qualifiers OR custom key generator class
            //TODO: feed cell tuples to a custom key generator class. Use return for outKey.
            //TODO:      Default key generator should work off row_key qual argument.
            //TODO: implementation and thorough testing of line parsing.
            //TODO:      Add fast-fail checks on the cols parameter to job run().
            context.write(outKey, outVal);
        }
    }

    public static class IngestKeyValueReducer
            extends Reducer<Text, CellValues, Key, Value> {

        private Key outKey;
        private Value outValue = new Value();
        private String colfam;

        @Override
        protected void reduce(Text key, Iterable<CellValues> values,
                              Context context) throws IOException, InterruptedException {
            int found = 0;
            for (CellValues value : values) {
                for (int i = 0; i < value.getCellValues().length; i++) {
                    String cell = value.getCellValues()[i];
                    String cellVis = getCellVis(i);
                    if(cellVis != null) {
                        outKey = new Key(key, new Text(colfam),
                                new Text(getQual(i)),
                                new Text(cellVis) );
                    } else {
                        outKey = new Key(key, new Text(colfam),
                                new Text(getQual(i)));
                    }

                    outValue.set(cell.getBytes());
                    context.write(outKey, outValue);
                }
            }
            if(found > 0)
                context.getCounter("Delimited Text Ingest", "duplicates").increment(found);
        }

        private String getQual(int i) {
            return null;  //To change body of created methods use File | Settings | File Templates.
        }

        private String getColFam() {
            return null;  //To change body of created methods use File | Settings | File Templates.
        }

        private String getCellVis(int i) {
            return null;
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
        GenericOptionsParser parser = new GenericOptionsParser(args);
        args = parser.getRemainingArgs();
        Configuration conf = parser.getConfiguration();
        ToolRunner.run(new DelimitedTextIngester(conf), args);
    }
}
