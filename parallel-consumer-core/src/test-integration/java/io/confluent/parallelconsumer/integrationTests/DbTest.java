
/*-
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.parallelconsumer.integrationTests;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulate real forward pressure, back pressure and error conditions by testing against a real database
 */
@Slf4j
public class DbTest extends KafkaTest<String, String> {

    final static protected PostgreSQLContainer dbc;

    /**
     * https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/#singleton-containers
     * https://github.com/testcontainers/testcontainers-java/pull/1781
     */
    static {
        dbc = new PostgreSQLContainer<>()
                .withReuse(true);
        dbc.start();
    }

    Connection connection;

    @BeforeEach
    public void followDbLogs() {
        if (log.isDebugEnabled()) {
            Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
            dbc.followOutput(logConsumer);
        }
    }

    @SneakyThrows
    @BeforeEach
    public void setupDatabase() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(dbc.getJdbcUrl());
        dataSource.setUser(dbc.getUsername());
        dataSource.setPassword(dbc.getPassword());

        connection = dataSource.getConnection();
//        connection = DriverManager.getConnection(dbc.getJdbcUrl(), dbc.getUsername(), dbc.getPassword());

        PreparedStatement create_table = connection.prepareStatement("""
                CREATE TABLE IF NOT EXISTS DATA(
                   ID SERIAL PRIMARY KEY     NOT NULL,
                   KEY           TEXT    NOT NULL,
                   VALUE         TEXT     NOT NULL
                );
                """);
        create_table.execute();
    }

    @SneakyThrows
    @Test
    public void testDatabaseSetup() {
        assertThat(dbc.isRunning()).isTrue(); // sanity

        savePayload("a", "test");
    }

    @SneakyThrows
    void savePayload(String key, String payload) {
        var query = "insert into data(key, value) values(?, ?)";
        PreparedStatement pst = connection.prepareStatement(query);
        pst.setString(1, key);
        pst.setString(2, payload);
        int i = pst.executeUpdate();
    }
}
