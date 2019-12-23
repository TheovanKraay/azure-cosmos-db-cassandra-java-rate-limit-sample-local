package com.azure.cosmosdb.cassandra.examples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.cosmosdb.cassandra.repository.UserRepository;
import com.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.github.javafaker.Faker;


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
    public static final int NUMBER_OF_WRITES_PER_THREAD = 30;
    public static final int MAX_NUMBER_OF_RETRIES = 20;
    

    //set instance variables using AtomicInteger to ensure thread safety
    AtomicInteger recordcount = new AtomicInteger(0);
    AtomicInteger exceptioncount = new AtomicInteger(0);
    AtomicInteger ratelimitcount = new AtomicInteger(0);
    AtomicInteger totalRetries = new AtomicInteger(0);

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        Session cassandraSession = utils.getSession();
        UserProfile u = new UserProfile();
        
        try {
            UserRepository repository = new UserRepository(cassandraSession);        
            // Drop keyspace in cassandra database if exists
            repository.deleteTable("DROP KEYSPACE IF EXISTS uprofile");
            Thread.sleep(2000);

            // Create keyspace in cassandra database
            repository.createKeyspace("CREATE KEYSPACE uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }");
            Thread.sleep(5000);

            // Create table in cassandra database
            repository.createTable("CREATE TABLE uprofile.user (user_id text PRIMARY KEY, user_name text, user_bcity text)");
            Thread.sleep(5000);

            LOGGER.info("inserting records....");
            
            // Insert rows into user table, passing number of threads, writes, and allowed number of retries per write
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
            LOGGER.info("count of record insert attempts logged in load test: " + u.recordcount);
            LOGGER.info("count of exceptions during load test: " + u.exceptioncount);
            LOGGER.info("count of exceptions that were caught and parsed as rate limits: " + u.ratelimitcount);   
            LOGGER.info("count of rate limit retries: " + u.totalRetries);         
        }
    }

    public static Random getRandom() {
        return random;
    }

    public void loadTest(UserRepository repository, UserProfile u, String preparedStatementString, int noOfThreads,
            int noOfWritesPerThread, int noOfRetries) {
        PreparedStatement preparedStatement = repository.prepareInsertStatement(preparedStatementString);
        Faker faker = new Faker();
        for (int i = 1; i <= noOfThreads; i++) {
            Runnable task = () -> {
                ;
                // String threadName = Thread.currentThread().getName();
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    UUID guid = java.util.UUID.randomUUID();
                    try {
                        String name = faker.name().lastName();
                        String city = faker.address().city();
                        u.recordcount.incrementAndGet();
                        //uncomment the below and comment retryUser() to observe the difference if retry with back off is not used
                        //repository.insertUser(preparedStatement, guid.toString(), name, city);
                        u.retryUser(guid.toString(), name, city, repository, preparedStatement, noOfRetries, 1, u, false);
                    } 
                    catch (Exception e) {
                        u.exceptioncount.incrementAndGet();;
                        System.out.println("Exception: " + e);
                    }
                }
            };
            Thread thread = new Thread(task);
            thread.start();
        }
    }
    public void retryUser(String id, String name, String city, UserRepository repository, PreparedStatement preparedStatement, int retries, int retry, UserProfile u, boolean stop) throws InterruptedException {
        if (retry >= retries){
            stop = true;
        }
        while(stop==false){
            try{
                repository.insertUser(preparedStatement, id, name,
                city);
            }
            catch (OverloadedException e) {
                retry++;
                try{
                    int retryWaitTime = getRetryAfterMs(e.toString(), retry);                        
                    u.ratelimitcount.incrementAndGet();;
                    Thread.sleep(retryWaitTime);               
                    System.out.println("429 error: rate limited. Waited " + retryWaitTime + " milliseconds before retrying");
                    System.out.println("trying " + retry + " of "+retries+ " times");
                    try{
                        //recursively retry until break condition is met (stop = true)                                                                        
                        u.retryUser(id, name, city, repository, preparedStatement, retries, retry, u, stop);
                    }
                    catch(Exception exx){
                        u.exceptioncount.incrementAndGet();;
                        System.out.println("retry failed: "+exx);
                    }     
                }
                catch (Exception ex){
                    System.out.println("could not do retry from OverloadedException catch block: " + ex);
                }                                         
            }      
            catch(NoHostAvailableException ex){
                retry++;
                System.out.println("Caught NoHostAvailableException: " + ex);
                System.out.println("Inner Exception of NoHostAvailableException: " + ex.getCause());
                System.out.println("retrying " + retry + " of "+retries+ "times after NoHostAvailableException");
                
                //retrying with exponential back-off if NoHostAvailableException caught
                int waitTimeInMilliSeconds = (int) Math.pow(2, retry) * 100;
                Thread.sleep(waitTimeInMilliSeconds);
                u.retryUser(id, name, city, repository, preparedStatement, retries, retry, u, stop);
                u.exceptioncount.incrementAndGet();;
            }      
            catch(Exception e){
                u.exceptioncount.incrementAndGet();;
                retry++;
                System.out.println("Exception: "+e);
            }
            finally{          
                u.totalRetries.set(u.totalRetries.get() + retry -1);       
                stop = true;
            }
        }   
    }

    public int getRetryAfterMs(String exceptionString, int retry){
        //parse the exception test to get retry milliseconds
        int millseconds = 0;
        String[] exceptions = exceptionString.toString().split(",");
        String[] retryProperty = exceptions[1].toString().split("=");
        exceptionString = retryProperty[0].toString().trim();
        if (exceptionString.equals("RetryAfterMs")){
            String value = retryProperty[1]; 
            millseconds = Integer.parseInt(value);
        }
        else{
            //default to an exponential back off if RetryAfterMs property not found
            millseconds = (int) Math.pow(2, retry) * 100;
        }
        return millseconds;
    }
}
