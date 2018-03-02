package cz.mirek.cassandra.domain;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table("sample")
public class Sample {
    @PrimaryKey
    private String id;

    public String getId() {
        return this.id;
    }
}
