package org.cassandraunit;

import com.datastax.driver.core.ResultSet;
import org.cassandraunit.dataset.CQLDataSet;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Test;
import org.junit.Before;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static org.junit.Assert.assertEquals;

/**
 * 
 * @author Marcin Szymaniuk
 * @author Jeremy Sevellec
 *
 */
public class CQLDataLoadTestWithClassInheritance extends AbstractCassandraUnit4CQLTestCase {

    @Before
    public void initialize() {
        System.setProperty("cassandra.unsafesystem", "true");
        int EMBEDDED_TIME_OUT = 90000;
        EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
        System.out.println("1 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);
    }

    @Override
    public CQLDataSet getDataSet() {
        return new ClassPathCQLDataSet("cql/simple.cql", "mykeyspace");
    }


    @Test
	public void testCQLDataAreInPlace() throws Exception {
        test();

	}

    @Test
    public void sameTestToMakeSureMultipleTestsAreFine() throws Exception {
        test();

    }

    private void test() {
        ResultSet result = getSession().execute("select * from testCQLTable WHERE id=1690e8da-5bf8-49e8-9583-4dff8a570737");

        String val = result.iterator().next().getString("value");
        assertEquals("Cql loaded string",val);
    }

}
