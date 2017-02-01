package org.cassandraunit;

import com.datastax.driver.core.ResultSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Before;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static org.junit.Assert.assertEquals;

/**
 * 
 * @author Jeremy Sevellec
 *
 */
public class CQLDataLoadTestWithMultiLineCQLStatements {
    @Before
    public void initialize() {
        System.setProperty("cassandra.unsafesystem", "true");
        int EMBEDDED_TIME_OUT = 90000;
        EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
        System.out.println("1 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);
    }

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql/multiLineStatements.cql", "mykeyspace"));


    @Test
	public void testCQLDataAreInPlace() throws Exception {
        test();

	}

    @Test
    public void sameTestToMakeSureMultipleTestsAreFine() throws Exception {
        test();

    }

    private void test() {
        ResultSet result = cassandraCQLUnit.session.execute("select * from testCQLTable WHERE id=1690e8da-5bf8-49e8-9583-4dff8a570737");

        String val = result.iterator().next().getString("value");
        assertEquals("Cql loaded string",val);
    }

}
