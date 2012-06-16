import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;


public class NGramJob implements Tool{

    private Configuration conf;

    public static final String NAME = "ngram";
    private static final String GRAM_LENGTH = "number_of_grams";

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
         if(args.length != 3) {
             System.err.println("Usage: ngram <input> <output> <number_of_grams>");
             System.exit(1);
         }
        ToolRunner.run(new NGramJob(new Configuration()), args);
    }

    public NGramJob(Configuration conf) {
        this.conf = conf;
    }


    public int run(String[] args) throws Exception {
        conf.setInt(GRAM_LENGTH, Integer.parseInt(args[2]));

        Job job = new Job(conf, "NGrams");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(NGramJob.NGramMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(NGramJob.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, removeAndSetOutput(args[1]));

        return job.waitForCompletion(true) ? 1: 0;
    }

    private Path removeAndSetOutput(String outputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputDir);
        fs.delete(path, true);
        return path;
    }

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private int gram_length;
        private Pattern space_pattern = Pattern.compile("[ ]");
        private StringBuilder gramBuilder = new StringBuilder();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           gram_length = context.getConfiguration().getInt(NGramJob.GRAM_LENGTH, 0);
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] tokens = space_pattern.split(value.toString());
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i];
                gramBuilder.setLength(0);
                if(i + gram_length <= tokens.length) {
                   for(int j = i; j < i + gram_length; j++) {
                       gramBuilder.append(tokens[j]);
                       gramBuilder.append(" ");
                   }
                   context.write(new Text(gramBuilder.toString()), NullWritable.get());
                }
            }

        }

    }
}
