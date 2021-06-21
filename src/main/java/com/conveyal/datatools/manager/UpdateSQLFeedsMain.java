package com.conveyal.datatools.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.conveyal.datatools.manager.DataManager.initializeApplication;
import static com.conveyal.datatools.manager.DataManager.registerRoutes;

/**
 * Main method that performs batch SQL updates on optionally filtered set of namespaces over the GTFS SQL database
 * connection specified in the configuration files (env.yml).
 *
 * For example, this script can add a column for all `routes` tables for schemas that do not have a null value for
 * the filename field in the feeds table. In effect, this would alter only those feeds that are editor snapshots.
 *
 * Argument descriptions:
 * 1. path to env.yml
 * 2. path to server.yml
 * 3. string update sql statement to apply to optionally filtered feeds (this should contain a namespace wildcard
 *    {@link UpdateSQLFeedsMain#NAMESPACE_WILDCARD} string for the namespace argument substitution).
 * 4. string field to filter feeds on
 * 5. string value (corresponding to field in arg 3) to filter feeds on (omit to use NULL as value or comma separate to
 *    include multiple values)
 * 6. boolean (optional) whether to run SQL as a test run (i.e., rollback changes and do not commit). If missing, this
 *    defaults to true.
 *
 * Sample arguments:
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename
 *
 * "/path/to/config/env.yml" "/path/to/config/server.yml" "alter table %s.routes add column some_column_name int" filename /tmp/gtfs.zip
 */
public class UpdateSQLFeedsMain {
    public static final String NAMESPACE_WILDCARD = "#ns#";

    public static void main(String[] args) throws IOException, SQLException {
        // First, set up application.
        initializeApplication(args);
        // Register HTTP endpoints so that the status endpoint is available during migration.
        registerRoutes();
        // Load args (first and second args are used for config files).
        // Update SQL string should be contained within third argument with %s specifier where namespace should be
        // substituted.
        String updateSql = args[2];
        // The next arguments will apply a where clause to conditionally to apply the updates.
        String field = args.length > 3 ? args[3] : null;
        String valuesArg = args.length > 4 ? args[4] : null;
        String[] values;
        // Set value to null if the string value = "null".
        if ("null".equals(valuesArg) || valuesArg == null) values = null;
        else values = valuesArg.split(",");
        // If test run arg is not included, default to true. Else, only set to false if value equals false.
        boolean testRun = args.length <= 5 || !"false".equals(args[5]);
        List<String> failedNamespace = updateFeedsWhere(updateSql, field, values, testRun);
        System.out.println("Finished!");
        System.out.println("Failed namespaces: " + String.join(", ", failedNamespace));
        System.exit(0);
    }

    /**
     * Applies the update SQL to feeds/namespaces based on the conditional expression provided by the field/values inputs.
     * If testRun is true, all changes applied to database will be rolled back at the end of execution.
     */
    private static List<String> updateFeedsWhere(String updateSql, String field, String[] values, boolean testRun) throws SQLException {
        if (updateSql == null) throw new RuntimeException("Update SQL must not be null!");
        // Keep track of failed namespaces for convenient printing at end of method.
        List<String> failedNamespace = new ArrayList<>();
        // Select feeds migrated from MapDB
        String selectFeedsSql = "select namespace from feeds";
        if (field != null) {
            // Add where in clause if field is not null
            // NOTE: if value is null, where clause will be executed accordingly (i.e., WHERE field = null)
            String operator = values == null
                ? ""
                : String.format("in (%s)", String.join(", ", Collections.nCopies(values.length, "?")));
            selectFeedsSql = String.format("%s where %s %s", selectFeedsSql, field, operator);
        }
        Connection connection = DataManager.GTFS_DATA_SOURCE.getConnection();
        if (!testRun) {
            System.out.println("Auto-committing each statement");
            // Set auto-commit to true.
            connection.setAutoCommit(true);
        } else {
            System.out.println("TEST RUN. Changes will NOT be committed (a rollback occurs at the end of method).");
        }
        PreparedStatement selectStatement = connection.prepareStatement(selectFeedsSql);
        if (values != null) {
            // Set filter values if not null (otherwise, IS NULL has already been populated).
            int oneBasedIndex = 1;
            for (String value : values) {
                selectStatement.setString(oneBasedIndex++, value);
            }
        }
        System.out.println(selectStatement.toString());
        ResultSet resultSet = selectStatement.executeQuery();
        int successCount = 0;
        while (resultSet.next()) {
            // Use the string found in the result as the table prefix for the following update query.
            String namespace = resultSet.getString(1);
            String updateTableSql = updateSql.replaceAll(NAMESPACE_WILDCARD, namespace);
            Statement statement = connection.createStatement();
            try {
                System.out.println(updateTableSql);
                int updated = statement.executeUpdate(updateTableSql);
                System.out.println(String.format("Updated rows: %d", updated));
                successCount++;
            } catch (SQLException e) {
                // The stops table likely did not exist for the schema.
                e.printStackTrace();
                failedNamespace.add(namespace);
            }
        }
        System.out.println(String.format("Updated %d tables.", successCount));
        // No need to commit the transaction because of auto-commit above (in fact, "manually" committing is not
        // permitted with auto-commit enabled.
        if (testRun) {
            // Rollback changes if performing a test run.
            System.out.println("Rolling back changes...");
            connection.rollback();
        }
        connection.close();
        return failedNamespace;
    }
}
