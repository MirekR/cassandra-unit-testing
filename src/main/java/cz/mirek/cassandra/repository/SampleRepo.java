package cz.mirek.cassandra.repository;

import cz.mirek.cassandra.domain.Sample;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SampleRepo extends CrudRepository<Sample, String> {
}
