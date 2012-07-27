package main.java.employee;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class EmployeeRDFTextOutputFormat extends
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

        public EmployeeRDFVertexWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(
                Vertex<Text, IntWritable, NullWritable, ?> vertex)
                throws IOException, InterruptedException {

            valOut.set(vertex.getValue().toString());
            if(vertex.getValue().get() == Integer.MAX_VALUE)
                valOut.set("no path");
            getRecordWriter().write(vertex.getId(), valOut);
        }


    }

    @Override
    public VertexWriter<Text, IntWritable, NullWritable>
    createVertexWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
        return new EmployeeRDFVertexWriter(recordWriter);
    }
}
