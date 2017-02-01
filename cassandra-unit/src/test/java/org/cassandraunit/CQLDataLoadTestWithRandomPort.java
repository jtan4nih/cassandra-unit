package org.cassandraunit;

import static org.junit.Assert.assertEquals;

import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.ResultSet;

@Ignore("May not start multiple cassandras with different configuration in one JVM")
public class CQLDataLoadTestWithRandomPort {

	@Before
    public void initialize() {
		System.setProperty("cassandra.unsafesystem", "true");
    	int EMBEDDED_TIME_OUT = 90000;
        EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
        System.out.println("1 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);
    }

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/simple.cql", "mykeyspace"), 
            EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

    @Test
	public void testNativeDriverAccessToRandomPort() throws Exception {
        ResultSet result = cassandraCQLUnit.session.execute("select * from testCQLTable WHERE id=1690e8da-5bf8-49e8-9583-4dff8a570737");

        String val = result.iterator().next().getString("value");
        assertEquals("Cql loaded string",val);
    }
}
