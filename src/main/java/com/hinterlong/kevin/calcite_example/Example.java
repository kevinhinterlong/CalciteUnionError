package com.hinterlong.kevin.calcite_example;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

/**
 * Example of Calcite Union not working
 *
 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/union.html">MYSQL Docs</a>
 */
public class Example {
    private static final String DATABASE_URL = "jdbc:h2:mem:test";
    private static String SCHEMA_NAME = "PUBLIC";
    private static String DRIVER = "org.h2.Driver";

    public static void main(String[] args) throws SQLException {
        Connection connection = makeDatabase();
        DataSource dataSource = JdbcSchema.dataSource(DATABASE_URL, DRIVER, null, null);

        System.out.println("WITHOUT  Order by and Limit");
        RelBuilder firstBuilder = getBuilder(dataSource, SCHEMA_NAME);
        RelNode firstNormal = fetchWithOrderAndLimit(firstBuilder);
        RelNode secondNormal = fetchWithOrderAndLimit(firstBuilder);
        queryManuallyAndWithBuilder(firstBuilder, connection, firstNormal, secondNormal);

        System.out.println("====================================================================");

        System.out.println("WITH Order by and Limit");
        RelBuilder secondBuilder = getBuilder(dataSource, SCHEMA_NAME);
        RelNode firstLimit = fetchWithOrderAndLimit(secondBuilder, 0, 10);
        RelNode secondLimit = fetchWithOrderAndLimit(secondBuilder, 10, 20);
        queryManuallyAndWithBuilder(secondBuilder, connection, firstLimit, secondLimit);
    }

    private static void queryManuallyAndWithBuilder(
            RelBuilder builder,
            Connection connection,
            RelNode first,
            RelNode second
    ) throws SQLException {
        // manually create union all.
        String manualSql = "(" + toSql(second, connection) + ")";
        manualSql += "\nUNION ALL\n";
        manualSql += "(" + toSql(first, connection) + ")";
        executeSql(connection, manualSql);

        // Test calcite union all
        builder.pushAll(Arrays.asList(first, second))
                .union(true);
        String generatedSql = toSql(builder.build(), connection);
        executeSql(connection, generatedSql);
    }

    private static RelNode fetchWithOrderAndLimit(RelBuilder builder) {
        return builder.scan("TEST")
                .project(builder.field("TEST_VALUE"))
                .build();
    }

    private static RelNode fetchWithOrderAndLimit(RelBuilder builder, int offset, int fetch) {
        return builder.scan("TEST")
                .project(builder.field("TEST_VALUE"))
                .sortLimit(offset, fetch, builder.field("TEST_VALUE"))
                .build();
    }

    private static void executeSql(Connection connection, String sql) {
        try {
            connection.createStatement().execute(sql);
            System.out.println("SUCCESS!");
        } catch (SQLException e) {
            System.out.println("FAILED! - " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String toSql(RelNode relNode, Connection connection) throws SQLException {
        SqlDialect dialect = SqlDialect.create(connection.getMetaData());
        SqlPrettyWriter sqlWriter = new SqlPrettyWriter(dialect);
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(dialect);

        SqlSelect sqlSelect = relToSqlConverter.visitChild(0, relNode).asSelect();
        return sqlWriter.format(sqlSelect);
    }

    private static Connection makeDatabase() throws SQLException {
        Connection connection = DriverManager.getConnection(DATABASE_URL);
        connection.createStatement().execute("CREATE TABLE TEST (TEST_VALUE INT)");

        String insertStatement = "INSERT INTO TEST (TEST_VALUE) VALUES(?)";

        for (int i = 0; i < 20; i++) {
            PreparedStatement preparedStatement = connection.prepareStatement(insertStatement);
            preparedStatement.setInt(1, i);
            preparedStatement.execute();
        }

        return connection;
    }

    public static RelBuilder getBuilder(DataSource dataSource, String schemaName) throws SQLException {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        return RelBuilder.create(
                Frameworks.newConfigBuilder()
                        .parserConfig(SqlParser.Config.DEFAULT)
                        .defaultSchema(
                                rootSchema.add(
                                        schemaName,
                                        JdbcSchema.create(rootSchema, null, dataSource, null, null)
                                )
                        )
                        .traitDefs((List<RelTraitDef>) null)
                        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
                        .build()
        );
    }
}
