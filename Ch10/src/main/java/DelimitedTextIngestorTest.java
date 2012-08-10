package mil.rebel.taint.accumulo;


import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.commons.cli.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;
import static mil.rebel.taint.accumulo.DelimitedTextIngester.*;

public class DelimitedTextIngestorTest {

    private DelimitedTextIngester job;
    private Configuration conf = new Configuration();

    @BeforeClass
    public void setup() {
        conf.set("mapred.job.tracker", "local");
    }


    @Test
    public void commandParams(){
        List<String> argsList = new ArrayList<String>();

        argsList.add("-in");
        argsList.add("fake_in");
        argsList.add("-out");
        argsList.add("fake_out");
        argsList.add("-ins");
        argsList.add("test");
        argsList.add("-table");
        argsList.add("blah");
        argsList.add("-u");
        argsList.add("user");
        argsList.add("-p");
        argsList.add("password");
        argsList.add("-zoo");
        argsList.add("localhost:2181");
        argsList.add("-cols");
        argsList.add("field1,field2");
        argsList.add("-cf");
        argsList.add("colfam");
        try {
            job = new DelimitedTextIngester(conf);
            CommandLine cmd = job.loadCommandOptions(argsList.toArray(new String[]{}));
            assertTrue(cmd.hasOption(INPUT_ARG));
            assertEquals(cmd.getOptionValue(INPUT_ARG), "fake_in");
            assertTrue(cmd.hasOption(USER_ARG));
            assertEquals(cmd.getOptionValue(USER_ARG), "user");

            argsList.add("-separator");
            argsList.add(";");
            cmd = job.loadCommandOptions(argsList.toArray(new String[]{}));
            assertTrue(cmd.hasOption(SEPARATOR_CONF_KEY));
            assertEquals(cmd.getOptionValue(SEPARATOR_CONF_KEY), ";");

        } catch (Exception e) {
            fail("EXCEPTION fail: " + e.getMessage());
        }

        try {
          job = new DelimitedTextIngester(conf);
            argsList.remove("-in");
            CommandLine cmd = job.loadCommandOptions(argsList.toArray(new String[]{}));
            fail("Should not have passed. Missing required input");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("missing required argument " + INPUT_ARG));
        } catch (ParseException e) {
           fail("EXCEPTION fail: " + e.getMessage());
        }
    }

}
