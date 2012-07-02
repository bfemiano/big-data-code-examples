import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;


public class NamedCountryOutputJob implements Tool{

    private Configuration conf;
    public static final String NAME = "named_output";


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new NamedCountryOutputJob(), args);
    }

    public int run(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("Usage: named_output <input> <output>");
            System.exit(1);
        }

        Job job = new Job(conf, "IP count by country to named files");
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(IPCountryMapper.class);
        job.setReducerClass(IPCountryReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setJarByClass(NamedCountryOutputJob.class);

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

    public static class IPCountryMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final int country_pos = 1;
        private static final Pattern pattern = Pattern.compile("\\t");

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String country = pattern.split(value.toString())[country_pos];
            context.write(new Text(country), new IntWritable(1));
        }
    }

    public static class IPCountryReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private MultipleOutputs output;

        @Override
        protected void setup(Context context
        ) throws IOException, InterruptedException {
            output = new MultipleOutputs(context);
        }


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int total = 0;
            for(IntWritable value: values) {
                total += value.get();
            }
            output.write(new Text("Output by MultipleOutputs"), NullWritable.get(), key.toString());
            output.write(key, new IntWritable(total), key.toString());
        }

        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            output.close();
        }
    }
}
