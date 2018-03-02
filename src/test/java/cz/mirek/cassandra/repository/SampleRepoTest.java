package cz.mirek.cassandra.repository;

import com.datastax.driver.core.Session;
import cz.mirek.cassandra.CassandraApplication;
import cz.mirek.cassandra.domain.Sample;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = CassandraApplication.class)
@ActiveProfiles("test")
public class SampleRepoTest {
    @Autowired
    private SampleRepo sampleRepo;

    @BeforeClass
    public static void initDb() throws IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 20000);

        Session session = EmbeddedCassandraServerHelper.getCluster().connect();
        session.execute("create keyspace  \"mirek\" WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};");
        session.execute("create table mirek.sample (id VARCHAR, PRIMARY KEY (id))");
        session.execute("insert into mirek.sample (id) values ('test')");
    }

    @Test
    public void shouldHaveSampleRepo() {
        assertThat(sampleRepo, notNullValue());
    }

    @Test
    public void shouldSelectTestObject() {
        Sample sample = sampleRepo.findOne("test");
        assertThat(sample, notNullValue());
        assertThat(sample.getId(), is("test"));
    }

    @Test
    public void shouldNotSelectTestObject() {
        Sample sample = sampleRepo.findOne("xxxx");
        assertThat(sample, nullValue());
    }
}