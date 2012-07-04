
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;


public class DistinctCounterJob implements Tool{

    private Configuration conf;
    public static final String NAME = "distinct_counter";
    public static final String COL_POS = "col_pos";


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DistinctCounterJob(), args);
    }

    public int run(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("Usage: distinct_counter <input> <output> <element_position>");
            System.exit(1);
        }
        conf.setInt(COL_POS, Integer.parseInt(args[2]));

        Job job = new Job(conf, "Count distinct elements at specified position");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setCombinerClass(DistinctReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJarByClass(DistinctCounterJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;

    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public static class DistinctMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static int col_pos;
        private static final Pattern pattern = Pattern.compile("\\t");
        private Text outKey = new Text();
        private static final IntWritable outValue = new IntWritable(1);

        @Override
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            col_pos = context.getConfiguration().getInt(DistinctCounterJob.COL_POS, 0);
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String field = pattern.split(value.toString())[col_pos];
            outKey.set(field);
            context.write(outKey, outValue);
        }
    }

    public static class DistinctReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable count = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int total = 0;
            for(IntWritable value: values) {
                total += value.get();
            }
            count.set(total);
            context.write(key, count);
        }
    }
}
