package org.cassandraunit.spring;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
// import org.junit.Before;

/**
 * @author Gaëtan Le Brun
 */
public class DummyCassandraConnector {

    private static int instancesCounter;
    private Session session;
    private Cluster cluster;

    public DummyCassandraConnector() {
        // System.setProperty("cassandra.unsafesystem", "true");
        // int EMBEDDED_TIME_OUT = 90000;
        // EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
        // System.out.println("4 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);

        instancesCounter++;
    }

    public static void resetInstancesCounter() {
        instancesCounter = 0;
    }

    public static int getInstancesCounter() {
        return instancesCounter;
    }


    public Session getSession() {
        return EmbeddedCassandraServerHelper.getSession();
    }
}
