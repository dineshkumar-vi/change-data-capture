package com.chassis.logminer.cdc.connectionfactory;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConnectionFactory implements IConnectionFactory {

    private DataSource dataSource() {
        final HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setJdbcUrl("jdbc:postgresql://localhost:5432/cdc_db");
        ds.setUsername("postgres");
        ds.setPassword("postgres");

        ds.setAutoCommit(true);
        ds.setMinimumIdle(2);
        ds.setMaximumPoolSize(10);
        ds.setConnectionTimeout(1000);

        return ds;
    }

    public Connection getConnection() throws SQLException {
        String dbURL = "jdbc:postgresql://localhost:5432/cdc_db";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "postgres");
        props.setProperty("replication", "database");
        props.setProperty("assumeMinServerVersion", "9.4");

        return DriverManager.getConnection(dbURL, props);
    }

    public static void main(String[] args) throws SQLException {
        PostgresConnectionFactory connectionFactory = new PostgresConnectionFactory();
        Connection connection = connectionFactory.getConnection();
        System.out.println("PostgreSQL connection established: " + connection);
        connection.close();
    }
}
