package nl.lawinegevaar.event.race;

import org.apache.commons.cli.*;
import org.firebirdsql.event.DatabaseEvent;
import org.firebirdsql.event.EventListener;
import org.firebirdsql.event.FBEventManager;
import org.firebirdsql.management.FBManager;

import java.sql.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Application to reproduce event race condition that causes lost events.
 *
 * @author Mark Rotteveel
 */
public class Application {

    //@formatter:off
    private static final String TABLE_DEF =
            "CREATE TABLE TEST ("
            + "     TESTVAL INTEGER NOT NULL"
            + ")";
    private static final String TRIGGER_DEF =
            "CREATE TRIGGER INSERT_TRIG "
            + "     FOR TEST AFTER INSERT "
            + "AS BEGIN "
            + "     POST_EVENT 'TEST_EVENT_A';"
            + "     POST_EVENT 'TEST_EVENT_B';"
            + "END";
    //@formatter:on

    private static final AtomicInteger INSERTS_DONE = new AtomicInteger();
    private static final AccumulatingEventListener LISTENER_A = new AccumulatingEventListener();
    private static final int FACTOR_A = 1;
    private static final AccumulatingEventListener LISTENER_B = new AccumulatingEventListener();
    private static final int FACTOR_B = 1;
    private static final FBEventManager EVENT_MANAGER = new FBEventManager();

    private static String hostName = "localhost";
    private static int portNumber = 3050;
    private static String databasePath = "D:/data/db/fb3/eventrace.fdb";
    private static String user = "sysdba";
    private static String password = "masterkey";
    private static String jdbcUrl;

    private static int threadCount = 8;
    private static int insertsPerThread = 200;
    private static CyclicBarrier startBarrier;

    public static void main(String[] args) throws Exception {
        if (!parseCommandLine(args)) {
            System.exit(-1);
        }

        System.out.printf("Using thread count %d, number of inserts per thread %d%n", threadCount, insertsPerThread);
        jdbcUrl = buildJdbcUrl();
        startBarrier = new CyclicBarrier(threadCount);

        createDatabase();
        setupDatabase();
        setupEventManager();
        // catch up
        Thread.sleep(500);

        Thread[] producerThreads = initializeInsertThreads();
        for (final Thread t : producerThreads) {
            // wait for producers to finish
            t.join();
        }

        // Allow event listeners to catch up
        Thread.sleep(1000);
        final int insertsDone = INSERTS_DONE.get();
        System.out.printf("Inserts done %d, matches expected %d: %s%n", insertsDone,
                threadCount * insertsPerThread, insertsDone == threadCount * insertsPerThread);
        final int totalEventsA = LISTENER_A.getTotalEvents();
        final int expectedA = FACTOR_A * insertsDone;
        System.out.printf("Event EVENT_NAME_A, expected %d, actual %d: %s%n", expectedA,
                totalEventsA, expectedA == totalEventsA ? "true" : "false <================");
        final int totalEventsB = LISTENER_B.getTotalEvents();
        final int expectedB = FACTOR_B * insertsDone;
        System.out.printf("Event EVENT_NAME_B, expected %d, actual %d: %s%n", expectedB,
                totalEventsB, expectedB == totalEventsB ? "true" : "false <================");
    }

    private static Thread[] initializeInsertThreads() {
        final Thread[] producerThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++){
            final Thread t = new Thread(new EventProducerThread(insertsPerThread));
            producerThreads[i] = t;
            t.start();
        }
        return producerThreads;
    }

    private static void setupEventManager() throws SQLException {
        EVENT_MANAGER.setHost(hostName);
        EVENT_MANAGER.setPort(portNumber);
        EVENT_MANAGER.setDatabase(databasePath);
        EVENT_MANAGER.setUser(user);
        EVENT_MANAGER.setPassword(password);
        EVENT_MANAGER.connect();
        EVENT_MANAGER.addEventListener("TEST_EVENT_A", LISTENER_A);
        EVENT_MANAGER.addEventListener("TEST_EVENT_B", LISTENER_B);
    }

    private static void setupDatabase() throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl + "?encoding=NONE", user, password);
             Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            stmt.execute(TABLE_DEF);
            stmt.execute(TRIGGER_DEF);
            connection.commit();
        }
    }

    private static void createDatabase() throws Exception {
        FBManager fbManager = new FBManager();
        fbManager.setServer(hostName);
        fbManager.setPort(portNumber);
        fbManager.setFileName(databasePath);
        fbManager.setUserName(user);
        fbManager.setPassword(password);
        fbManager.setForceCreate(true);
        fbManager.setCreateOnStart(true);
        fbManager.start();
    }

    private static Options createCommandLineOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("help").desc("Print usage").build());
        options.addOption(Option.builder().longOpt("host").hasArg(true).argName("HOSTNAME").desc("Server hostname").build());
        options.addOption(Option.builder().longOpt("port").hasArg(true).argName("PORTNUMBER").desc("Server port number").build());
        options.addOption(Option.builder().longOpt("database").hasArg(true).argName("DB_PATH").desc("Database path").build());
        options.addOption(Option.builder().longOpt("user").hasArg(true).argName("USER").desc("User").build());
        options.addOption(Option.builder().longOpt("password").hasArg(true).argName("PASSWORD").desc("Password").build());
        options.addOption(Option.builder("t").longOpt("threadCount").hasArg(true).argName("COUNT").desc("Number of insert threads").build());
        options.addOption(Option.builder("i").longOpt("insertsPerThread").hasArg(true).argName("COUNT").desc("Number of inserts per insert thread").build());
        return options;
    }

    private static boolean parseCommandLine(String[] args) {
        Options options = createCommandLineOptions();
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("help")) {
                printUsage(options);
                return false;
            }

            if (commandLine.hasOption("host")) {
                hostName = commandLine.getOptionValue("host");
            }

            if (commandLine.hasOption("port")) {
                portNumber = Integer.parseInt(commandLine.getOptionValue("port"));
            }

            if (commandLine.hasOption("database")) {
                databasePath = commandLine.getOptionValue("database");
            }

            if (commandLine.hasOption("user")) {
                user = commandLine.getOptionValue("user");
            }

            if (commandLine.hasOption("password")) {
                password = commandLine.getOptionValue("password");
            }

            if (commandLine.hasOption("threadCount")) {
                threadCount = Integer.parseInt(commandLine.getOptionValue("threadCount"));
            }

            if (commandLine.hasOption("insertsPerThread")) {
                insertsPerThread = Integer.parseInt(commandLine.getOptionValue("insertsPerThread"));
            }

            return true;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            printUsage(options);
            return false;
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("event-race-condition", options, true);
    }

    private static String buildJdbcUrl() {
        return "jdbc:firebirdsql://" + hostName + ":" + portNumber + "/" + databasePath;
    }

    static class AccumulatingEventListener implements EventListener {

        private volatile int eventCount = 0;

        public int getTotalEvents(){
            return eventCount;
        }

        public synchronized void eventOccurred(DatabaseEvent event){
            eventCount += event.getEventCount();
        }
    }

    static class EventProducerThread implements Runnable {

        private int count;

        public EventProducerThread(int insertCount){
            this.count = insertCount;
        }

        public void run() {
            try (Connection conn = DriverManager.getConnection(jdbcUrl + "?encoding=NONE", user, password);
                 PreparedStatement stmt = conn.prepareStatement("INSERT INTO TEST VALUES (?)")) {
                startBarrier.await();
                for (int i = 0; i < count; i++) {
                    stmt.setInt(1, i);
                    stmt.execute();
                    INSERTS_DONE.getAndIncrement();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
