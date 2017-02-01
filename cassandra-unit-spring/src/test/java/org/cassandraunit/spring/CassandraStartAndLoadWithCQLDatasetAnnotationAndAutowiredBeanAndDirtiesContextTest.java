package org.cassandraunit.spring;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.junit.Before;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.ResultSet;

/**
 * @author Gaëtan Le Brun
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/autowired-context.xml"},
    initializers = {CassandraStartAndLoadWithCQLDatasetAnnotationAndAutowiredBeanAndDirtiesContextTest.EnsureUniqueContext.class} )
@TestExecutionListeners({CassandraUnitDependencyInjectionTestExecutionListener.class, DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
@CassandraDataSet(value = {"cql/dataset1.cql"})
@EmbeddedCassandra
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class CassandraStartAndLoadWithCQLDatasetAnnotationAndAutowiredBeanAndDirtiesContextTest {

    @Before
    public void initialize() {
        System.setProperty("cassandra.unsafesystem", "true");
        int EMBEDDED_TIME_OUT = 90000;
        EmbeddedCassandraServerHelper.getCluster().getConfiguration().getSocketOptions().setReadTimeoutMillis(EMBEDDED_TIME_OUT);
        System.out.println("4 EmbeddedCassandraServerHelper EMBEDDED_TIME_OUT = " + EMBEDDED_TIME_OUT);
    }

    @Autowired
    private DummyCassandraConnector dummyCassandraConnector;

    @BeforeClass
    public static void beforeClass() {
        DummyCassandraConnector.resetInstancesCounter();
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
        ResultSet result = dummyCassandraConnector.getSession().execute("select * from testCQLTable1 WHERE id=1690e8da-5bf8-49e8-9583-4dff8a570717");
        String val = result.iterator().next().getString("value");
        assertEquals("1- Cql loaded string", val);
    }

    @AfterClass
    public static void afterClass(){
        assertEquals(2, DummyCassandraConnector.getInstancesCounter());
    }

    public static class EnsureUniqueContext implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext ctx) {
        }
    }
}
