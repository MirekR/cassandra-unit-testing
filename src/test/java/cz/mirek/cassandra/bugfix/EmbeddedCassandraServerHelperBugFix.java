package cz.mirek.cassandra.bugfix;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmbeddedCassandraServerHelperBugFix {
    private static Logger log = LoggerFactory.getLogger(EmbeddedCassandraServerHelper.class);
    public static final long DEFAULT_STARTUP_TIMEOUT = 10000L;
    public static final String DEFAULT_TMP_DIR = "target/embeddedCassandra";
    public static final String DEFAULT_CASSANDRA_YML_FILE = "cu-cassandra.yaml";
    public static final String CASSANDRA_RNDPORT_YML_FILE = "cu-cassandra-rndport.yaml";
    public static final String DEFAULT_LOG4J_CONFIG_FILE = "/log4j-embedded-cassandra.properties";
    private static final String INTERNAL_CASSANDRA_KEYSPACE = "system";
    private static final String INTERNAL_CASSANDRA_AUTH_KEYSPACE = "system_auth";
    private static final String INTERNAL_CASSANDRA_DISTRIBUTED_KEYSPACE = "system_distributed";
    private static final String INTERNAL_CASSANDRA_SCHEMA_KEYSPACE = "system_schema";
    private static final String INTERNAL_CASSANDRA_TRACES_KEYSPACE = "system_traces";
    private static CassandraDaemon cassandraDaemon = null;
    private static String launchedYamlFile;
    private static Cluster cluster;
    private static Session session;

    public EmbeddedCassandraServerHelperBugFix() {
    }

    public static void startEmbeddedCassandra() throws TTransportException, IOException, InterruptedException, ConfigurationException {
        startEmbeddedCassandra(10000L);
    }

    public static void startEmbeddedCassandra(long timeout) throws TTransportException, IOException, InterruptedException, ConfigurationException {
        startEmbeddedCassandra("cu-cassandra.yaml", timeout);
    }

    public static void startEmbeddedCassandra(String yamlFile) throws TTransportException, IOException, ConfigurationException {
        startEmbeddedCassandra(yamlFile, 10000L);
    }

    public static void startEmbeddedCassandra(String yamlFile, long timeout) throws TTransportException, IOException, ConfigurationException {
        startEmbeddedCassandra(yamlFile, "target/embeddedCassandra", timeout);
    }

    public static void startEmbeddedCassandra(String yamlFile, String tmpDir) throws TTransportException, IOException, ConfigurationException {
        startEmbeddedCassandra(yamlFile, tmpDir, 10000L);
    }

    public static void startEmbeddedCassandra(String yamlFile, String tmpDir, long timeout) throws TTransportException, IOException, ConfigurationException {
        if (cassandraDaemon == null) {
            if (!StringUtils.startsWith(yamlFile, "/")) {
                yamlFile = "/" + yamlFile;
            }

            rmdir(tmpDir);
            copy(yamlFile, tmpDir);
            File file = new File(tmpDir + yamlFile);
            readAndAdaptYaml(file);
            startEmbeddedCassandra(file, tmpDir, timeout);
        }
    }

    public static void startEmbeddedCassandra(File file, String tmpDir, long timeout) throws TTransportException, IOException, ConfigurationException {
        if (cassandraDaemon == null) {
            checkConfigNameForRestart(file.getAbsolutePath());
            log.debug("Starting cassandra...");
            log.debug("Initialization needed");
            System.setProperty("cassandra.config", "file:" + file.getAbsolutePath());
            System.setProperty("cassandra-foreground", "true");
            System.setProperty("cassandra.native.epoll.enabled", "false");
            if (System.getProperty("log4j.configuration") == null) {
                copy("/log4j-embedded-cassandra.properties", tmpDir);
                System.setProperty("log4j.configuration", "file:" + tmpDir + "/log4j-embedded-cassandra.properties");
            }

            cleanupAndLeaveDirs();
            final CountDownLatch startupLatch = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(new Runnable() {
                public void run() {
                    EmbeddedCassandraServerHelperBugFix.cassandraDaemon = new CassandraDaemon();
                    EmbeddedCassandraServerHelperBugFix.cassandraDaemon.activate();
                    startupLatch.countDown();
                }
            });

            try {
                long xxx = 201111;
                if (!startupLatch.await(xxx, TimeUnit.MILLISECONDS)) {
                    log.error("Cassandra daemon did not start after " + xxx + " ms. Consider increasing the timeout");
                    throw new AssertionError("Cassandra daemon did not start within timeout");
                }

                cluster = Cluster.builder().addContactPoints(new String[]{getHost()}).withPort(getNativeTransportPort()).build();
                session = cluster.connect();
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    public void run() {
                        EmbeddedCassandraServerHelperBugFix.session.close();
                        EmbeddedCassandraServerHelperBugFix.cluster.close();
                    }
                }));
            } catch (InterruptedException var10) {
                log.error("Interrupted waiting for Cassandra daemon to start:", var10);
                throw new AssertionError(var10);
            } finally {
                executor.shutdown();
            }

        }
    }

    private static void checkConfigNameForRestart(String yamlFile) {
        boolean wasPreviouslyLaunched = launchedYamlFile != null;
        if (wasPreviouslyLaunched && !launchedYamlFile.equals(yamlFile)) {
            throw new UnsupportedOperationException("We can't launch two Cassandra configurations in the same JVM instance");
        } else {
            launchedYamlFile = yamlFile;
        }
    }

    /** @deprecated */
    @Deprecated
    public static void stopEmbeddedCassandra() {
        log.warn("EmbeddedCassandraServerHelper.stopEmbeddedCassandra() is now deprecated, previous version was not fully operating");
    }

    public static void cleanEmbeddedCassandra() {
        dropKeyspaces();
    }

    public static void cleanDataEmbeddedCassandra(String keyspace, String... excludedTables) {
        if (hasHector()) {
            cleanDataWithHector(keyspace, excludedTables);
        } else {
            cleanDataWithNativeDriver(keyspace, excludedTables);
        }

    }

    public static Cluster getCluster() {
        return cluster;
    }

    public static Session getSession() {
        return session;
    }

    public static String getClusterName() {
        return DatabaseDescriptor.getClusterName();
    }

    public static String getHost() {
        return DatabaseDescriptor.getRpcAddress().getHostName();
    }

    public static int getRpcPort() {
        return DatabaseDescriptor.getRpcPort();
    }

    public static int getNativeTransportPort() {
        return DatabaseDescriptor.getNativeTransportPort();
    }

    private static void cleanDataWithNativeDriver(String keyspace, String... excludedTables) {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        Collection<TableMetadata> tables = keyspaceMetadata.getTables();
        List<String> excludeTableList = Arrays.asList(excludedTables);
        Iterator var5 = tables.iterator();

        while(var5.hasNext()) {
            TableMetadata table = (TableMetadata)var5.next();
            String tableName = table.getName();
            if (!excludeTableList.contains(tableName)) {
                session.execute("truncate table " + tableName);
            }
        }

    }

    private static void cleanDataWithHector(String keyspace, String... excludedTables) {
        String host = DatabaseDescriptor.getRpcAddress().getHostName();
        int port = DatabaseDescriptor.getRpcPort();
        me.prettyprint.hector.api.Cluster cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator(host + ":" + port));
        KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keyspace);
        List<ColumnFamilyDefinition> cfDefs = keyspaceDefinition.getCfDefs();
        List<String> excludedTableList = Arrays.asList(excludedTables);
        Iterator var8 = cfDefs.iterator();

        while(var8.hasNext()) {
            ColumnFamilyDefinition columnFamilyDefinition = (ColumnFamilyDefinition)var8.next();
            String cfName = columnFamilyDefinition.getName();
            if (!excludedTableList.contains(cfName)) {
                cluster.truncate(keyspace, cfName);
            }
        }

    }

    private static boolean hasHector() {
        boolean hector = false;

        try {
            new CassandraHostConfigurator("");
            hector = true;
        } catch (NoClassDefFoundError var2) {
            hector = false;
        }

        return hector;
    }

    private static void dropKeyspaces() {
        if (hasHector()) {
            dropKeyspacesWithHector();
        } else {
            dropKeyspacesWithNativeDriver();
        }

    }

    private static void dropKeyspacesWithHector() {
        String host = DatabaseDescriptor.getRpcAddress().getHostName();
        int port = DatabaseDescriptor.getRpcPort();
        log.debug("Cleaning cassandra keyspaces on " + host + ":" + port);
        me.prettyprint.hector.api.Cluster cluster = HFactory.getOrCreateCluster("TestCluster", new CassandraHostConfigurator(host + ":" + port));
        List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
        Iterator var4 = keyspaces.iterator();

        while(var4.hasNext()) {
            KeyspaceDefinition keyspaceDefinition = (KeyspaceDefinition)var4.next();
            String keyspaceName = keyspaceDefinition.getName();
            if (!isSystemKeyspaceName(keyspaceName)) {
                cluster.dropKeyspace(keyspaceName);
            }
        }

    }

    private static void dropKeyspacesWithNativeDriver() {
        List<String> keyspaces = new ArrayList();
        Iterator var1 = cluster.getMetadata().getKeyspaces().iterator();

        while(var1.hasNext()) {
            KeyspaceMetadata keyspace = (KeyspaceMetadata)var1.next();
            if (!isSystemKeyspaceName(keyspace.getName())) {
                keyspaces.add(keyspace.getName());
            }
        }

        var1 = keyspaces.iterator();

        while(var1.hasNext()) {
            String keyspace = (String)var1.next();
            session.execute("DROP KEYSPACE " + keyspace);
        }

    }

    private static boolean isSystemKeyspaceName(String keyspaceName) {
        return "system".equals(keyspaceName) || "system_auth".equals(keyspaceName) || "system_distributed".equals(keyspaceName) || "system_schema".equals(keyspaceName) || "system_traces".equals(keyspaceName);
    }

    private static void rmdir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(new File(dir));
        }

    }

    private static void copy(String resource, String directory) throws IOException {
        mkdir(directory);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator") + fileName);
        InputStream is = EmbeddedCassandraServerHelper.class.getResourceAsStream(resource);
        Throwable var5 = null;

        try {
            OutputStream out = new FileOutputStream(file);
            Throwable var7 = null;

            try {
                byte[] buf = new byte[1024];

                int len;
                while((len = is.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }

                out.close();
            } catch (Throwable var31) {
                var7 = var31;
                throw var31;
            } finally {
                if (out != null) {
                    if (var7 != null) {
                        try {
                            out.close();
                        } catch (Throwable var30) {
                            var7.addSuppressed(var30);
                        }
                    } else {
                        out.close();
                    }
                }

            }
        } catch (Throwable var33) {
            var5 = var33;
            throw var33;
        } finally {
            if (is != null) {
                if (var5 != null) {
                    try {
                        is.close();
                    } catch (Throwable var29) {
                        var5.addSuppressed(var29);
                    }
                } else {
                    is.close();
                }
            }

        }
    }

    private static void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }

    private static void cleanupAndLeaveDirs() throws IOException {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog commitLog = CommitLog.instance;
//        try {
//            Thread.sleep(2000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        commitLog.resetUnsafe(true);
    }

    private static void cleanup() throws IOException {
        String[] directoryNames = new String[]{DatabaseDescriptor.getCommitLogLocation()};
        String[] var1 = directoryNames;
        int var2 = directoryNames.length;

        int var3;
        String dirName;
        File dir;
        for(var3 = 0; var3 < var2; ++var3) {
            dirName = var1[var3];
            dir = new File(dirName);
            if (!dir.exists()) {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }

            FileUtils.deleteRecursive(dir);
        }

        var1 = DatabaseDescriptor.getAllDataFileLocations();
        var2 = var1.length;

        for(var3 = 0; var3 < var2; ++var3) {
            dirName = var1[var3];
            dir = new File(dirName);
            if (!dir.exists()) {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }

            FileUtils.deleteRecursive(dir);
        }

    }

    public static void mkdirs() {
        DatabaseDescriptor.createAllDirectories();
    }

    private static void readAndAdaptYaml(File cassandraConfig) throws IOException {
        String yaml = readYamlFileToString(cassandraConfig);
        Pattern portPattern = Pattern.compile("^([a-z_]+)_port:\\s*([0-9]+)\\s*$", 8);
        Matcher portMatcher = portPattern.matcher(yaml);
        StringBuffer sb = new StringBuffer();

        boolean replaced;
        String replacement;
        for(replaced = false; portMatcher.find(); portMatcher.appendReplacement(sb, replacement)) {
            String portName = portMatcher.group(1);
            int portValue = Integer.parseInt(portMatcher.group(2));
            if (portValue == 0) {
                portValue = findUnusedLocalPort();
                replacement = portName + "_port: " + portValue;
                replaced = true;
            } else {
                replacement = portMatcher.group(0);
            }
        }

        portMatcher.appendTail(sb);
        if (replaced) {
            writeStringToYamlFile(cassandraConfig, sb.toString());
        }

    }

    private static String readYamlFileToString(File yamlFile) throws IOException {
        FileReader reader = new FileReader(yamlFile);
        Throwable var2 = null;

        try {
            StringBuffer sb = new StringBuffer();
            char[] cbuf = new char[1024];

            for(int readden = reader.read(cbuf); readden >= 0; readden = reader.read(cbuf)) {
                sb.append(cbuf, 0, readden);
            }

            String var6 = sb.toString();
            return var6;
        } catch (Throwable var15) {
            var2 = var15;
            throw var15;
        } finally {
            if (reader != null) {
                if (var2 != null) {
                    try {
                        reader.close();
                    } catch (Throwable var14) {
                        var2.addSuppressed(var14);
                    }
                } else {
                    reader.close();
                }
            }

        }
    }

    private static void writeStringToYamlFile(File yamlFile, String yaml) throws IOException {
        Writer writer = new OutputStreamWriter(new FileOutputStream(yamlFile), "utf-8");
        Throwable var3 = null;

        try {
            writer.write(yaml);
        } catch (Throwable var12) {
            var3 = var12;
            throw var12;
        } finally {
            if (writer != null) {
                if (var3 != null) {
                    try {
                        writer.close();
                    } catch (Throwable var11) {
                        var3.addSuppressed(var11);
                    }
                } else {
                    writer.close();
                }
            }

        }

    }

    private static int findUnusedLocalPort() throws IOException {
        ServerSocket serverSocket = new ServerSocket(0);
        Throwable var1 = null;

        int var2;
        try {
            var2 = serverSocket.getLocalPort();
        } catch (Throwable var11) {
            var1 = var11;
            throw var11;
        } finally {
            if (serverSocket != null) {
                if (var1 != null) {
                    try {
                        serverSocket.close();
                    } catch (Throwable var10) {
                        var1.addSuppressed(var10);
                    }
                } else {
                    serverSocket.close();
                }
            }

        }

        return var2;
    }
}
