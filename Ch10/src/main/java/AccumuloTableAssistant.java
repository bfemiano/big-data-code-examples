package examples.accumulo;


import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import javax.management.StringValueExp;
import javax.print.DocFlavor;
import java.io.*;
import java.util.*;


public class AccumuloTableAssistant {

    private Connector connector;
    private String tableName;
    private String user;
    private String password;
    private String zooQuorum;
    private String instanceName;

    private final Logger logger = Logger.getLogger(AccumuloTableAssistant.class);
    private static final String CHARSET = "ISO-8859-1";

    private AccumuloTableAssistant() {} //prevent accidental instantiation

    private AccumuloTableAssistant(Builder builder) {
        tableName = builder.tableName;
        user = builder.user;
        password = builder.password;
        zooQuorum = builder.zooQuorum;
        instanceName = builder.instanceName;
        if(tableName == null | user == null | password == null | zooQuorum == null | instanceName == null)
            throw new IllegalArgumentException("required argument is null. " +
                    "You may have forgotten to call a Builder setter (tableName, user, password, zooQuorum, instanceName");
    }

    private void init()
            throws AccumuloException, AccumuloSecurityException{
        connector = new ZooKeeperInstance(instanceName, zooQuorum).getConnector(user, password);
    }


    public boolean tableExists()
            throws AccumuloException{
        return getTableOpts().exists(tableName);
    }

    public void createTableIfNotExists()
            throws AccumuloException, AccumuloSecurityException {
        try {
            if(!tableExists()) {
                getTableOpts().create(tableName);
            }
        } catch (TableExistsException e) {
            logger.error("Table still exists", e);
        }
    }

    public void deleteTableIfExists()
            throws AccumuloException,AccumuloSecurityException {
        try {
            if(tableExists()) {
                getTableOpts().delete(tableName);
            }
        } catch (TableNotFoundException e) {
            logger.error("Table not found", e);
        }
    }

    public int presplitAndWriteHDFSFile(Configuration conf, String localFilePath, String hdfsFilePath)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        TreeSet<Text> splitsToAdd = new TreeSet<Text>();
        FSDataOutputStream outStream = null;
        try {
            String[] splits = getSplitsFromLocalFile(localFilePath);
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(hdfsFilePath);
            fs.delete(path, true);
            outStream = fs.create(path, true);
            for(String s : splits) {
                byte[] splitBytes = s.getBytes(CHARSET);
                splitsToAdd.add(new Text(splitBytes));
                outStream.writeBytes(new String(Base64.encodeBase64(splitBytes)));
                getTableOpts().addSplits(tableName, splitsToAdd);
            }
        } catch (IOException e) {
            logger.error(e);
        } finally {
            try {
                if(outStream != null)
                    outStream.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }
        return splitsToAdd.size();
    }

    private String[] getSplitsFromLocalFile(String localFilePath)
            throws IOException{
        List<String> splitsList = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader((localFilePath)));
            String line;
            while((line = reader.readLine()) != null) {
                splitsList.add(line);
            }
        } catch (FileNotFoundException e){
            logger.error("local split file not found " + localFilePath, e);
        }
        return splitsList.toArray(new String[]{});
    }

    public void loadImportDirectory(Configuration conf, String dir)
            throws AccumuloException, AccumuloSecurityException, IOException, TableNotFoundException{
        FileSystem fs = FileSystem.get(conf);
        Path failures = new Path(dir, "failures");
        fs.delete(failures, true);
        fs.mkdirs(new Path(dir, "failures"));
        if(!dir.endsWith("/"))
             dir += "/";
        getTableOpts().importDirectory(tableName, dir, dir + "failures", false);
    }

    public TableOperations getTableOpts()
            throws AccumuloException{
        return connector.tableOperations();
    }


    public static class Builder {
        private String tableName;
        private String user;
        private String password;
        private String zooQuorum;
        private String instanceName;

        public AccumuloTableAssistant build()
                throws AccumuloException, AccumuloSecurityException{
            AccumuloTableAssistant assistant = new AccumuloTableAssistant(this);
            assistant.init();
            return assistant;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setInstanceName(String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        public Builder setZooQuorum(String zooQuorum) {
            this.zooQuorum = zooQuorum;
            return this;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }
    }
}
