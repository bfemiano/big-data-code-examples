package examples.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Collection;

import static org.testng.Assert.*;

public class AccumuloTableAssistantTest {

    private AccumuloTableAssistant assistant;
    private Configuration conf = new Configuration();

    @BeforeClass
    public void setup() throws AccumuloException, AccumuloSecurityException {
        assistant = new AccumuloTableAssistant.Builder().
                setInstanceName("test").setPassword("password").setUser("root")
                .setTableName("blah").setZooQuorum("localhost:2181").build();
    }

    @Test
    public void tableCreateAndDelete() {
        try {
            assistant.createTableIfNotExists();
            assertTrue(assistant.tableExists());
            assistant.deleteTableIfExists();
            assertTrue(!assistant.tableExists());
        } catch (AccumuloException e){
            fail("Create table failed" + e.getMessage());
        } catch (AccumuloSecurityException e) {
            fail("Create table failed" + e.getMessage());
        }
    }

    @Test
    public void presplit() {
        try {
            File localSplitFile = new File("splits.txt");
            FileWriter splitWriter = new FileWriter(localSplitFile);
            splitWriter.write("000001_zzzzz\n");
            splitWriter.write("999999_zzzzz");
            splitWriter.close();
            Path path = new Path("/tmp/splits.txt");
            assistant.createTableIfNotExists();
            int numSplits = assistant.presplitAndWriteHDFSFile(conf, localSplitFile.getAbsolutePath(), path.toUri().toString());
            assertEquals(numSplits, 2);

            Collection<Text> splitsFromTableOpt = assistant.getTableOpts().getSplits("blah");
            assertEquals(splitsFromTableOpt.size(), numSplits);
            assistant.deleteTableIfExists();
            assertFalse(assistant.tableExists());

            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while((line = reader.readLine()) != null) {
                line = new String(Base64.decodeBase64(line));
                assertEquals(line, "000001_zzzzz999999_zzzzz");
            }

        } catch (IOException e) {
            fail(e.getMessage());
        }  catch (AccumuloException e) {
            fail("EXCEPTION fail: " + e.getMessage());
        } catch (AccumuloSecurityException e) {
            fail("EXCEPTION fail: " + e.getMessage());
        } catch (TableNotFoundException e) {
            fail("EXCEPTION fail: " + e.getMessage());
        }
    }
}
