package main.java.employee;

import com.google.common.collect.Maps;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;


public class EmployeeRDFTextInputFormat extends
        TextVertexInputFormat<Text, IntWritable, NullWritable,
            IntWritable> {

  @Override
  public VertexReader<Text, IntWritable, NullWritable, IntWritable>
  createVertexReader(InputSplit split, TaskAttemptContext context)
    throws IOException {
    return new EmployeeRDFVertexReader(
        textInputFormat.createRecordReader(split, context));
  }

  /**
   * Vertex reader associated with {@link EmployeeRDFTextInputFormat}.
   */
  public static class EmployeeRDFVertexReader extends
      TextVertexInputFormat.TextVertexReader<Text, IntWritable,
      NullWritable, IntWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern TAB = Pattern.compile("[\\t]");
    private static final Pattern COLON = Pattern.compile("[:]");
    private static final Pattern COMMA = Pattern.compile("[,]");

    /**
     * Constructor with the line reader.
     *
     * @param lineReader Internal line reader.
     */
    public EmployeeRDFVertexReader(RecordReader<LongWritable, Text>
    lineReader) {
      super(lineReader);
    }

    @Override
    public Vertex<Text, IntWritable, NullWritable, IntWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<Text, IntWritable, NullWritable, IntWritable>
      vertex = BspUtils.<Text, IntWritable, NullWritable,
      IntWritable>createVertex(getContext().getConfiguration());

      String[] tokens = TAB.split(getRecordReader()
          .getCurrentValue().toString());
      Text vertexId = new Text(tokens[0]);

      IntWritable value = new IntWritable(0);
      String subtoken = COLON.split(tokens[2])[1];
      String[] subs = COMMA.split(subtoken);
      Map<Text, NullWritable> edges =
          Maps.newHashMapWithExpectedSize(subs.length);
      for(String sub : subs) {
         if(!sub.equals("none"))
            edges.put(new Text(sub), NullWritable.get());
      }

      vertex.initialize(vertexId, value, edges, null);

      return vertex;
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }
  }
}
