package com.azure.cosmosdb.cassandra.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.azure.cosmosdb.cassandra.repository.UserRepository;
import com.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.OverloadedException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate load testing and handling of rate limiting (429 error)
 * in Cassandra API using RetryAfterMs property returned from server to determine back-off wait time.
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);
    private static Random random = new Random();
    public static final int NUMBER_OF_THREADS = 10;
    public static final int NUMBER_OF_WRITES_PER_THREAD = 10;
    public static final int MAX_NUMBER_OF_RETRIES = 4;

    //set instance variables
    int recordcount = 0;
    int exceptioncount = 0;
    int ratelimitcount = 0;

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        Session cassandraSession = utils.getSession();
        UserProfile u = new UserProfile();
        try {
            UserRepository repository = new UserRepository(cassandraSession);        
            // Create keyspace in cassandra database
            repository.deleteTable("DROP KEYSPACE IF EXISTS uprofile");
            repository.createKeyspace("CREATE KEYSPACE IF NOT EXISTS uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }");

            Thread.sleep(5000);
            // Create table in cassandra database
            repository.createTable("CREATE TABLE IF NOT EXISTS uprofile.user (user_id text PRIMARY KEY, user_name text, user_bcity text)");
            Thread.sleep(5000);

            LOGGER.info("inserting records....");
            // Insert rows into user table
            u.loadTest(repository, u, "INSERT INTO uprofile.user (user_id, user_name , user_bcity) VALUES (?,?,?)", NUMBER_OF_THREADS,
            NUMBER_OF_WRITES_PER_THREAD, MAX_NUMBER_OF_RETRIES);

        } catch (Exception e) {
            System.out.println("Second Exception " + e);
        } finally {
            Thread.sleep(5000);
            CassandraUtils utilsfinal = new CassandraUtils();
            Session cassandraSessionFinal = utilsfinal.getSession();
            UserRepository repositoryfinally = new UserRepository(cassandraSessionFinal);
            repositoryfinally.selectUserCount("SELECT COUNT(*) as count FROM uprofile.user");
            utils.close();
            LOGGER.info("count of exceptions: " + u.exceptioncount);
            LOGGER.info("count of rate limits: " + u.ratelimitcount);
            LOGGER.info("count of records attempted: " + u.recordcount);
        }
    }

    public static Random getRandom() {
        return random;
    }

    public void loadTest(UserRepository repository, UserProfile u, String preparedStatementString, int noOfThreads,
            int noOfWritesPerThread, int noOfRetries) {
        PreparedStatement preparedStatement = repository.prepareInsertStatement(preparedStatementString);
        for (int i = 1; i <= noOfThreads; i++) {
            Runnable task = () -> {
                ;
                // String threadName = Thread.currentThread().getName();
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    UUID guid = java.util.UUID.randomUUID();
                    try {
                        u.recordcount++;
                        //repository.insertUser(preparedStatement, guid.toString(), "x" + guid,"y" + guid);
                        u.retry(guid.toString(), repository, preparedStatement, noOfRetries, 1, u, false);
                    } 
                    catch (Exception e) {
                        u.exceptioncount++;
                        System.out.println("Exception: " + e);
                    }
                }
            };
            Thread thread = new Thread(task);
            thread.start();
        }
    }
    public void retry(String guid, UserRepository repository, PreparedStatement preparedStatement, int retries, int retry, UserProfile u, boolean stop) throws InterruptedException {
        if (retry > retries){
            stop = true;
        }
        while(stop==false){
            try{
                repository.insertUser(preparedStatement, guid, "x" + guid,
                "y" + guid);
            }
            catch (OverloadedException e) {
                retry++;
                try{
                    String[] exceptions = e.toString().split(",");
                    String[] retryProperty = exceptions[1].toString().split("=");
                    if (retryProperty[0].toString().trim().equals("RetryAfterMs")){
                        String value = retryProperty[1];                         
                        u.ratelimitcount++;
                        Thread.sleep(Integer.parseInt(value));
                        System.out.println("429 error: rate limited. Waited " + value + " milliseconds before retrying");
                        try{
                            //recursively call retry until break condition is met (stop = true)                  
                            u.retry(guid, repository, preparedStatement, retries, retry, u, stop);
                        }
                        catch(Exception exx){
                            System.out.println("retry failed: "+exx);
                        }
                    }        
                }
                catch (Exception ex){
                    System.out.println("could not do retry from OverloadedException catch block: " + ex);
                }                                         
            }            
            catch(Exception e){
                u.exceptioncount++;
                retry++;
                System.out.println("Exception: "+e);
            }
            finally{
                stop = true;
            }
        }   
    }
}
