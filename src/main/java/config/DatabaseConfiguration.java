package config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.flyway.FlywayFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DatabaseConfiguration extends Configuration {
    @NotNull
    @Valid
    private DataSourceFactory dataSourceFactory
            = new DataSourceFactory();

    @JsonProperty("database")
    public DataSourceFactory getDataSourceFactory() {
        return dataSourceFactory;
    }


    @NotNull
    @Valid
    private FlywayFactory flywayFactory
            = new FlywayFactory();


    public FlywayFactory getFlywayFactory() {
        return flywayFactory;
    }

}
