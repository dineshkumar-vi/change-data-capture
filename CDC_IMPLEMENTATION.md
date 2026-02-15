# CDC (Change Data Capture) Implementation

This document describes the PostgreSQL and Oracle CDC implementation in identical architectural patterns.

## Overview

This project implements Change Data Capture (CDC) for both PostgreSQL and Oracle databases using a unified architecture pattern. Both implementations follow the same structure for consistency and maintainability.

## Architecture

Both PostgreSQL and Oracle CDC implementations follow an identical pattern:

```
CDC Main Class → Initializer → CDC Task
       ↓              ↓            ↓
Connection Factory  Statements  Log/WAL Files
```

## PostgreSQL CDC Implementation

### Package Structure
```
com.chassis.logminer.cdc.postgres/
├── PostgresCDC.java              (Main CDC orchestrator)
├── AbstractPostgresCDC.java      (Abstract base class)
├── IPostgresCDC.java             (Interface)
├── PostgresInitializer.java      (Initialization logic)
├── PostgresStatements.java       (SQL statements)
├── PostgresCDCTask.java          (Change processing)
└── PostgresWalFile.java          (WAL file representation)
```

### Key Components

#### 1. PostgresCDC.java
Main entry point for PostgreSQL CDC. It:
- Establishes connection using PostgresConnectionFactory
- Initializes replication slot and gets current LSN (Log Sequence Number)
- Retrieves WAL (Write-Ahead Log) files
- Starts logical replication stream
- Processes changes through PostgresCDCTask
- Cleans up resources

#### 2. PostgresInitializer.java
Handles initialization tasks:
- Gets current LSN from database
- Creates or retrieves replication slot
- Lists available WAL files
- Retrieves database OID

#### 3. PostgresStatements.java
Contains all SQL statements for PostgreSQL operations:
- LSN queries
- Replication slot management
- WAL file listing
- Publication management
- Table metadata queries

#### 4. PostgresCDCTask.java
Processes CDC events:
- Reads from replication stream
- Parses operation types (INSERT, UPDATE, DELETE, COMMIT)
- Filters tables based on include list
- Sends feedback to PostgreSQL server

#### 5. PostgresWalFile.java
Represents a WAL file with:
- File name
- Location (LSN)
- Size

### PostgreSQL CDC Flow

```
1. Connect to PostgreSQL with replication mode enabled
2. Create/Get replication slot (logical replication)
3. Get current LSN position
4. List available WAL files
5. Start logical replication stream with pgoutput plugin
6. Read and process changes:
   - Parse JSON/binary messages
   - Identify operation type (I/U/D/C)
   - Filter by table name
   - Print change details
7. Send LSN feedback to server
8. Close stream and connection
```

### Configuration
```properties
Database URL: jdbc:postgresql://localhost:5432/cdc_db
Username: postgres
Password: postgres
Replication Mode: database
Plugin: pgoutput
Slot Name: cdc_slot
```

## Oracle CDC Implementation

### Package Structure
```
com.chassis.logminer.cdc.oracle/
├── OracleCDC.java                (Main CDC orchestrator)
├── AbstractOracleCDC.java        (Abstract base class)
├── IOracleCDC.java               (Interface)
├── OracleInitializer.java        (Initialization logic)
├── OracleStatements.java         (SQL statements)
├── OracleCDCTask.java            (Change processing)
├── OracleLogFile.java            (Log file representation)
└── OracleScn.java                (SCN representation)
```

### Key Components

#### 1. OracleCDC.java
Main entry point for Oracle CDC. It:
- Establishes connection using OracleConnectionFactory
- Initializes LogMiner with redo thread and SCN
- Retrieves archived and online redo logs
- Adds log files to LogMiner
- Starts LogMiner session with SCN range
- Processes changes through OracleCDCTask
- Stops LogMiner and cleans up

#### 2. OracleInitializer.java
Handles initialization tasks:
- Gets current redo thread
- Retrieves first available SCN from archive logs
- Lists archived and online redo logs
- Filters duplicate sequences

#### 3. OracleStatements.java
Contains all SQL statements for Oracle operations:
- LogMiner start/stop procedures
- Log file addition procedures
- SCN and redo log queries
- Instance information queries
- Archive log queries

#### 4. OracleCDCTask.java
Processes CDC events:
- Queries V$LOGMNR_CONTENTS for changes
- Filters by operation code (1=INSERT, 2=UPDATE, 3=DELETE, 5=DDL, 7=COMMIT, 36=ROLLBACK)
- Builds object ID predicates for table filtering
- Handles multi-line SQL redo statements (CSF flag)
- Parses schema and table names

#### 5. OracleLogFile.java
Represents a redo log file with:
- File name
- First SCN and Next SCN
- Sequence number
- Type (ARCHIVE or REDO)
- Current flag
- Thread number

#### 6. OracleScn.java
System Change Number implementation:
- Wraps BigInteger for large SCN values
- Supports arithmetic operations (add, subtract)
- Comparable interface for ordering
- Special values (NULL, MAX, ONE)

### Oracle CDC Flow

```
1. Connect to Oracle database
2. Get current redo thread number
3. Get first available SCN from archive logs
4. List archived and online redo logs
5. Add log files to LogMiner session
6. Start LogMiner with SCN range
7. Query V$LOGMNR_CONTENTS for changes:
   - Filter by operation codes (1,2,3,5,7,36)
   - Filter by schema/table names
   - Filter by object IDs
   - Handle multi-line SQL redo
8. Process each change record
9. Stop LogMiner
10. Close connection
```

### Configuration
```properties
Database URL: jdbc:oracle:thin:@localhost:1521:sid
Username: C##ORACDC
Password: Test123456
Options: DICT_FROM_ONLINE_CATALOG + COMMITTED_DATA_ONLY
```

## Connection Factories

### PostgresConnectionFactory
Located in: `com.chassis.logminer.cdc.connectionfactory.PostgresConnectionFactory`

Features:
- Uses PostgreSQL JDBC driver
- Enables replication mode
- Configures connection properties for logical replication
- Uses HikariCP for connection pooling

### OracleConnectionFactory
Located in: `com.chassis.logminer.cdc.connectionfactory.OracleConnectionFactory`

Features:
- Uses Oracle JDBC driver (ojdbc11)
- Standard JDBC connection
- Uses HikariCP for connection pooling

## Unified Architecture Patterns

Both implementations share:

1. **Interface-based design**: `IPostgresCDC` / `IOracleCDC`
2. **Abstract base class**: `AbstractPostgresCDC` / `AbstractOracleCDC`
3. **Main CDC class**: Entry point with `startCDC()` method
4. **Initializer class**: Database-specific initialization
5. **Statements class**: SQL statement repository
6. **Task class**: Change processing logic
7. **Log file class**: Log/WAL file representation
8. **Connection factory**: Database connection management

## Dependencies

### PostgreSQL
```gradle
implementation group: 'org.postgresql', name: 'postgresql', version: '42.6.0'
```

### Oracle
```gradle
implementation group: 'com.oracle.database.jdbc', name: 'ojdbc11', version: '21.9.0.0'
```

### Common
```gradle
implementation group: 'com.zaxxer', name: 'HikariCP', version: '5.0.1'
implementation group: 'org.apache.commons', name: 'commons-lang3', version: '3.12.0'
```

## Running the CDC Applications

### PostgreSQL CDC
```bash
./gradlew run --args="com.chassis.logminer.cdc.postgres.PostgresCDC"
```

Or run the main method directly:
```java
PostgresCDC postgresCDC = new PostgresCDC();
postgresCDC.startCDC();
```

### Oracle CDC
```bash
./gradlew run --args="com.chassis.logminer.cdc.oracle.OracleCDC"
```

Or run the main method directly:
```java
OracleCDC oracleCDC = new OracleCDC();
oracleCDC.startCDC();
```

## Prerequisites

### PostgreSQL
1. PostgreSQL 9.4 or higher
2. Logical replication enabled (`wal_level = logical`)
3. Replication slot created
4. User with replication privileges
5. Publication created for monitored tables

### Oracle
1. Oracle Database 11g or higher
2. Archive log mode enabled
3. Supplemental logging enabled
4. LogMiner privileges granted to user
5. Access to V$LOGMNR_CONTENTS view

## Table Filtering

Both implementations support table filtering using schema.table notation:

### PostgreSQL Example
```java
List<String> includeList = new ArrayList<>();
includeList.add("public.cdc_test");
```

### Oracle Example
```java
List<String> includeList = new ArrayList<>();
includeList.add("C##CRUDUSER.CDC_TEST");
```

## Operation Type Mapping

| Operation | PostgreSQL | Oracle |
|-----------|------------|--------|
| INSERT    | I / BEGIN+INSERT | 1 |
| UPDATE    | U / UPDATE | 2 |
| DELETE    | D / DELETE | 3 |
| DDL       | - | 5 |
| COMMIT    | COMMIT | 7 |
| ROLLBACK  | - | 36 |

## Error Handling

Both implementations include:
- Connection error handling
- SQL exception handling
- Resource cleanup in finally blocks
- Thread interruption handling
- Timeout management

## Performance Considerations

### PostgreSQL
- Stream read timeout: 10 seconds
- Maximum records per session: 10,000
- LSN feedback frequency: After each message
- Replication slot prevents WAL cleanup

### Oracle
- SCN range: 10,000,000 per session
- Batch size for log addition: All available logs
- Dictionary mode: Online catalog
- Committed data only for consistency

## Monitoring

### PostgreSQL
Monitor:
- Replication lag: `SELECT * FROM pg_replication_slots`
- WAL disk usage
- Slot active status

### Oracle
Monitor:
- LogMiner sessions: `V$LOGMNR_SESSION`
- Archive log generation rate
- SCN progression rate
- V$LOGMNR_CONTENTS query performance

## Future Enhancements

1. Add proper message decoders (ProtoBuf for PostgreSQL, XML/JSON for Oracle)
2. Implement offset persistence (resume from last position)
3. Add metrics and monitoring
4. Implement backpressure handling
5. Add configurable batch processing
6. Support for multiple tables simultaneously
7. Add schema change detection and handling
8. Implement proper transaction boundary handling
9. Add integration tests
10. Create Spring Boot auto-configuration

## Conclusion

Both PostgreSQL and Oracle CDC implementations follow identical architectural patterns, making them easy to understand, maintain, and extend. The unified structure allows developers to work with either database using familiar patterns and conventions.
