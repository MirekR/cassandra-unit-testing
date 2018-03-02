package cz.mirek.cassandra.repository;

import cz.mirek.cassandra.CassandraApplication;
import cz.mirek.cassandra.domain.Sample;
import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnit;
import org.cassandraunit.spring.CassandraUnitDependencyInjectionTestExecutionListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = CassandraApplication.class)
@ActiveProfiles("test")
@TestExecutionListeners({ CassandraUnitDependencyInjectionTestExecutionListener.class, DependencyInjectionTestExecutionListener.class })
@CassandraDataSet(value = { "cql/sample-table.cql", "cql/sample-tabledata.cql" }, keyspace = "mirek")
@CassandraUnit
public class SampleRepoTest {
    @Autowired
    private SampleRepo sampleRepo;

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