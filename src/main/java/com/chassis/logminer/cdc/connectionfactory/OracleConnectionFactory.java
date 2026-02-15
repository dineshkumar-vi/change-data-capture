package com.chassis.logminer.cdc.connectionfactory;

import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnectionFactory implements IConnectionFactory {

    private DataSource dataSource() {
        final HikariDataSource ds = new HikariDataSource();
        ds.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        ds.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:sid");
        ds.setUsername("system");
        ds.setPassword("Test@123456");

        ds.setAutoCommit(true);
        ds.setMinimumIdle(2);
        ds.setMaximumPoolSize(10);
        ds.setConnectionTimeout(1000);

        return ds;
    }

    public Connection getConnection() throws SQLException {
        String dbURL = "jdbc:oracle:thin:@localhost:1521:sid";
        return DriverManager.getConnection(dbURL, "C##ORACDC", "Test123456");
    }

    public static void main(String[] args) throws SQLException {
        OracleConnectionFactory connectionFactory = new OracleConnectionFactory();
        Connection connection = connectionFactory.getConnection();
        System.out.println("Oracle connection established: " + connection);
        connection.close();
    }
}
