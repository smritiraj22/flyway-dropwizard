package main;

import config.DatabaseConfiguration;
import io.dropwizard.Application;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import io.dropwizard.flyway.FlywayBundle;
import io.dropwizard.flyway.FlywayFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.h2.tools.DeleteDbFiles;

import java.sql.*;

public class DataBaseMain extends Application<DatabaseConfiguration> {
    @Override
    public void run(DatabaseConfiguration databaseConfiguration, Environment environment) throws Exception {

        FlywayFactory flywayFactory = databaseConfiguration.getFlywayFactory();
        ManagedDataSource dataSource = databaseConfiguration.getDataSourceFactory().build(environment.metrics(), "datasource");
        Flyway flyway = flywayFactory.build(dataSource);
        DeleteDbFiles.execute("~", "flywaytestdb", true);
        flyway.migrate();
        Class.forName(databaseConfiguration.getDataSourceFactory().getDriverClass());
        Connection connection = DriverManager.getConnection(databaseConfiguration.getDataSourceFactory().getUrl(),"","");

        String SelectQuery = "select * from PERSON";
        PreparedStatement selectPreparedStatement = connection.prepareStatement(SelectQuery);
        ResultSet rs = selectPreparedStatement.executeQuery();
        System.out.println("H2 Database inserted through PreparedStatement");
        while (rs.next()) {
            System.out.println("Id "+rs.getInt("id")+" Name "+rs.getString("name"));
        }
        selectPreparedStatement.close();
        connection.commit();

        DeleteDbFiles.execute("~", "flywaytestdb", true);
    }

    public static void main(String[] args) throws Exception {
        new DataBaseMain().run(args);
    }

    /*public void startDb(DatabaseConfiguration databaseConfiguration) {
        File databaseDir = new File("java.io.tmpdir", "mysql-mxj");
        int portNumber = Integer.parseInt(System.getProperty("c-mxj_test_port",
                "3336"));
        MysqldResource mysqldResource = new MysqldResource(databaseDir);
        Map database_options = new HashMap();
        database_options.put(MysqldResourceI.PORT, Integer.toString(portNumber));
        database_options.put(MysqldResourceI.INITIALIZE_USER, "true");
        database_options.put(MysqldResourceI.INITIALIZE_USER_NAME, databaseConfiguration.getDataSourceFactory().getUser());
        database_options.put(MysqldResourceI.INITIALIZE_PASSWORD, databaseConfiguration.getDataSourceFactory().getPassword());

        mysqldResource.start("flyway-drop-thread", database_options);
    }
*/
    public void initialize(Bootstrap<DatabaseConfiguration> bootstrap) {
        bootstrap.addBundle(new FlywayBundle<DatabaseConfiguration>() {
            @Override
            public DataSourceFactory getDataSourceFactory(DatabaseConfiguration configuration) {
                return configuration.getDataSourceFactory();
            }

            @Override
            public FlywayFactory getFlywayFactory(DatabaseConfiguration configuration) {
                return configuration.getFlywayFactory();
            }
        });
    }
}
