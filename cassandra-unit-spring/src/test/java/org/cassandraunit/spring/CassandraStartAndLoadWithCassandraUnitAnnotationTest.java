package org.cassandraunit.spring;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Before;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static org.junit.Assert.assertEquals;

/**
 * @author Olivier Bazoud
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraUnit
public class CassandraStartAndLoadWithCassandraUnitAnnotationTest {

  @Before
  public void initialize() {
      System.setProperty("cassandra.unsafesystem", "true");
      int EMBEDDED_TIME_OUT = 90000;
      EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
      System.out.println("4 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);
  }

  @Test
  public void should_work() {
    test();
  }

  @Test
  public void should_work_twice() {
    test();
  }

  private void test() {
    Session session = EmbeddedCassandraServerHelper.getSession();
    ResultSet result = session.execute("select * from testCQLTable WHERE id=1690e8da-5bf8-49e8-9583-4dff8a570737");
    String val = result.iterator().next().getString("value");
    assertEquals("Cql loaded string", val);
  }

}
