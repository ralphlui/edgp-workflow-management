package sg.edu.nus.iss.edgp.workflow.management.service.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import sg.edu.nus.iss.edgp.workflow.management.exception.DynamicSQLServiceException;
import sg.edu.nus.iss.edgp.workflow.management.repository.DynamicSQLRepository;
import sg.edu.nus.iss.edgp.workflow.management.service.IDynamicSQLService;

@RequiredArgsConstructor
@Service
public class DynamicSQLService implements IDynamicSQLService {

    private static final Logger logger = LoggerFactory.getLogger(DynamicSQLService.class);

    private static final Set<String> STATIC_COLS = Set.of(
            "id", "created_date", "updated_date", "is_archived"
    );

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final DynamicSQLRepository dynamicSQLRepository;

    @Override
    public void buildCreateTableSQL(Map<String, Object> data, String tableName) {
        try {
            if (tableName == null || tableName.isEmpty()) {
                throw new DynamicSQLServiceException("Table Name is empty while creating master table");
            }

            // 0) Normalize table name and column keys
            tableName = tableName.toLowerCase();
            Map<String, Object> normalized = normalizePayload(data);

            // 1) Ensure table exists (with only static columns the first time)
            ensureTableExists(tableName);

            // 2) Evolve schema for new dynamic columns
            Set<String> existing = getExistingColumns(tableName);
            addMissingColumns(tableName, existing, normalized);

            // 3) Insert row
            insertData(tableName, normalized);

            logger.info("Upserted schema and inserted data for table `{}`", tableName);
        } catch (Exception e) {
            logger.error("An error occurred while creating/evolving clean data table.... {}", e.getMessage(), e);
            throw new DynamicSQLServiceException("An error occurred while creating clean data table", e);
        }
    }

    @Override
    public void insertData(String tableName, Map<String, Object> data) {
        try {
            tableName = tableName.toLowerCase();
            String schema;
            try (Connection c = jdbcTemplate.getDataSource().getConnection()) {
                schema = c.getCatalog(); // MySQL/MariaDB; adjust if needed for other DBs
            }

            if (!dynamicSQLRepository.tableExists(schema, tableName)) {
                throw new IllegalStateException("No table found. Please set up the table before uploading data.");
            }

            // Always add an id if not present
            data.putIfAbsent("id", UUID.randomUUID().toString());

            // Repository will execute a parameterized INSERT; schema already evolved above
            dynamicSQLRepository.insertRow(tableName, data);
            logger.info("Successfully inserted data into `{}`", tableName);
        } catch (Exception e) {
            logger.error("An error occurred while inserting clean data into the database.... {}", e.getMessage(), e);
            throw new DynamicSQLServiceException("An error occurred while inserting clean data", e);
        }
    }

    // ========== SCHEMA HELPERS ==========

    private void ensureTableExists(String tableName) {
        String staticColumns = String.join(", ",
            "`id` VARCHAR(36) PRIMARY KEY",
            "`created_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "`updated_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            "`is_archived` BOOLEAN DEFAULT FALSE"
        );
        String query = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" + staticColumns + ")";
        jdbcTemplate.execute(query);
    }

    private Set<String> getExistingColumns(String tableName) throws SQLException {
        DataSource ds = jdbcTemplate.getDataSource();
        Objects.requireNonNull(ds, "DataSource is null");
        Set<String> cols = new HashSet<>();
        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            // Note: for MySQL, catalog = db name; schemaPattern can be null
            try (ResultSet rs = meta.getColumns(conn.getCatalog(), null, tableName, null)) {
                while (rs.next()) {
                    cols.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }
        }
        return cols;
    }

    private void addMissingColumns(String tableName, Set<String> existing, Map<String, Object> data) {
        // Determine which dynamic columns are missing
        List<String> alters = new ArrayList<>();
        for (Map.Entry<String, Object> e : data.entrySet()) {
            String col = e.getKey(); // already normalized
            if (STATIC_COLS.contains(col)) continue;      // static columns exist already
            if (!existing.contains(col)) {
                String sqlType = mapDataType(e.getValue());
                alters.add("ALTER TABLE `" + tableName + "` ADD COLUMN `" + col + "` " + sqlType);
                existing.add(col);
            }
        }

        // Execute ALTERs one by one (simple and safe)
        for (String alter : alters) {
            jdbcTemplate.execute(alter);
            logger.info("Added missing column via: {}", alter);
        }
    }

    // ========== MAPPING / NORMALIZATION ==========

    private Map<String, Object> normalizePayload(Map<String, Object> data) {
        // Lowercase keys; space->underscore; guard against null keys
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : data.entrySet()) {
            if (e.getKey() == null) continue;
            String k = normalizeColumnName(e.getKey());
            normalized.put(k, e.getValue());
        }
        return normalized;
    }

    private String normalizeColumnName(String name) {
        return name.toLowerCase().trim().replace(' ', '_');
    }

    private String mapDataType(Object value) {
        if (value == null) return "VARCHAR(255)";

        Class<?> c = value.getClass();
        if (c == Integer.class || c == Short.class || c == Byte.class || c == Long.class) return "BIGINT";
        if (c == Double.class || c == Float.class || c == java.math.BigDecimal.class) return "DECIMAL(38,10)";
        if (c == Boolean.class) return "BOOLEAN";
        if (c == java.sql.Date.class || c == java.time.LocalDate.class) return "DATE";
        if (c == java.sql.Timestamp.class || c == java.time.LocalDateTime.class) return "TIMESTAMP";
        // Fallback
        return "VARCHAR(255)";
    }
}
