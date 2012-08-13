package examples.accumulo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * User: bfemiano
 * Date: 8/9/12
 * Time: 8:43 PM
 */
public class IngestLineParser {
        private Configuration conf;
        private CellValues tuple;

        private IngestLineParser() {}; //avoid configuration-less instantiation.

        public IngestLineParser(Configuration conf) {
            this.conf = conf;
        }

        public CellValues[] parse(LongWritable key, Text line)
                throws IOException {
            //TOOD: Read each qual/value pair from the line. assume the 'cf' column family. apply cell vis
            //      if necessary. Check line length matches expected number of qualifiers.
            //TODO: Support optionally ignoring delimiter token if between special character.
            //TODO: Needs tons of unit testing.
            return null;
        }
    }
