package ru.yoomoney.tech.dbqueue.test;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class DefaultDatabaseInitializer {
    private static JdbcTemplate pgJdbcTemplate;
    private static TransactionTemplate pgTransactionTemplate;

    public static synchronized void initialize() {
        if (pgJdbcTemplate != null) {
            return;
        }

        String postgresImage = "docker.nexus.yooteam.ru/" + PostgreSQLContainer.IMAGE + ":9.5";
        PostgreSQLContainer<?> dbContainer = new PostgreSQLContainer<>(DockerImageName
                .parse(postgresImage)
                .asCompatibleSubstituteFor("postgres"));

        dbContainer.withEnv("POSTGRES_INITDB_ARGS", "--nosync");
        dbContainer.withCommand("postgres -c fsync=off -c full_page_writes=off -c synchronous_commit=off");
        dbContainer.start();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(dbContainer.getJdbcUrl());
        dataSource.setPassword(dbContainer.getPassword());
        dataSource.setUser(dbContainer.getUsername());
        pgJdbcTemplate = new JdbcTemplate(dataSource);
        pgTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        pgTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        pgTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

    }

    public static void createTable(String ddlTemplate, String tableName) {
        initialize();
        executeDdl(String.format(ddlTemplate, tableName, tableName, tableName));
    }

    private static void executeDdl(String ddl) {
        initialize();
        getTransactionTemplate().execute(status -> {
            getJdbcTemplate().execute(ddl);
            return new Object();
        });
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        return pgJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return pgTransactionTemplate;
    }
}
