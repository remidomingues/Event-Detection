package ClusteringEvaluator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeMap;

public class DBManager {

    // -------------------------------------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------------------------------------
    public static Connection connection = null;

    /*
     * SQL key words
     */
    public static final String SELECT = "SELECT";
    public static final String FROM = "FROM";
    public static final String WHERE = "WHERE";
    public static final String ORDER_BY = "ORDER BY";
    public static final String INSERT_INTO = "INSERT INTO";
    public static final String VALUES = "VALUES";
    public static final String UPDATE = "UPDATE";
    public static final String SET = "SET";
    public static final String AND = "AND";
    public static final String OR = "OR";
    public static final String DELETE = "DELETE";
    public static final String TRUNCATE_TABLE = "TRUNCATE TABLE";
    public static final String ERROR = "Error";
    public static final String BETWEEN = "BETWEEN";

    // -------------------------------------------------------------------------------------------------------
    // Connection/Init
    // -------------------------------------------------------------------------------------------------------
    public static void ConnectToDB(String path, int timeout) throws ClassNotFoundException, SQLException {
        // load the sqlite-JDBC driver using the current class loader
        Class.forName("org.sqlite.JDBC");

        try {
            // create a database connection
            connection = DriverManager.getConnection(path);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(timeout);  // set timeout to 30 sec.
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

        // -------------------------------------------------------------------------------------------------------
        // READER SERVICES
        // -------------------------------------------------------------------------------------------------------

        /**
         * Performs a Select From Where request on the database
         * 
         * @param connection
         *            Connection to the database
         * @param select
         *            String after the SELECT word of a SQL query
         * @param from
         *            String after the FROM word of a SQL query
         * @param where
         *            String after the WHERE word of a SQL query
         * @param orderBy
         *            String after the ORDER BY word of a SQL query
         */
    

    static ResultSet sfwQuery(Connection connection, String select,
            String from, String where, String orderBy) {

        // Build Query
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(SELECT + " ");
        queryBuilder.append(select);
        queryBuilder.append(" " + FROM + " ");
        queryBuilder.append(from);
        if (where != null) {
            queryBuilder.append(" " + WHERE + " ");
            queryBuilder.append(where);
        }
        if (orderBy != null) {
            queryBuilder.append(" " + ORDER_BY + " ");
            queryBuilder.append(orderBy);
        }

        // Execute Query

        ResultSet rs = null;
        Statement statement;
        try {
            statement = connection.createStatement();
            rs = statement.executeQuery(queryBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * Builds a string for a join between two tables
     * 
     * @param table1
     *            First table to join
     * @param key1
     *            Key of the first table
     * @param table2
     *            Second table to join
     * @param key2
     *            Key of the second table
     */
    public static String buildJoin(String table1, String key1, String table2,
            String key2) {
        StringBuilder where = new StringBuilder();
        where.append(table1 + "." + key1 + " = " + table2 + "." + key2);
        return where.toString();
    }

    // -------------------------------------------------------------------------------------------------------
    // WRITER SERVICES
    // -------------------------------------------------------------------------------------------------------
    /**
     * Deletes a tuple from the base
     * 
     * @param from
     *            String after the FROM word of a SQL query
     * @param where
     *            String after the FROM word of a SQL query
     */
    static void delete(Connection connection, String from, String where) {

        // Build Query
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(DELETE);

        queryBuilder.append(" " + FROM + " ");
        queryBuilder.append(from);
        if (where != null) {
            queryBuilder.append(" " + WHERE + " ");
            queryBuilder.append(where);
        }

        // Execute Query
        Statement statement;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(queryBuilder.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Truncates a table
     * 
     * @param connection
     *            Connection to the database
     * @param table
     *            Table to truncate
     */
    static void truncateTable(Connection connection, String table) {

        StringBuilder query = new StringBuilder();
        query.append(TRUNCATE_TABLE + " " + table);

        Statement statement;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inserts a tuple in a table
     * 
     * @param connection
     *            Connection to the database
     * @param table
     *            Table to insert into
     * @param tuple
     *            tuple to insert
     */
    static boolean insertInto(Connection connection, String table,
            TreeMap<String, Object> tuple) {

        // Build columns
        StringBuilder query = new StringBuilder();
        query.append(INSERT_INTO + " " + table);
        query.append(" (");

        int i = 0;
        for (String key : tuple.keySet()) {
            query.append(key);
            if (i < tuple.keySet().size() - 1) {
                query.append(", ");
            }
            i++;
        }

        query.append(") ");

        // Build values
        query.append(VALUES);
        query.append(" (");

        i = 0;
        for (String key : tuple.keySet()) {
            if (tuple.get(key) instanceof String) {
                query.append("'");
                query.append(tuple.get(key));
                query.append("'");
            } else {
                query.append(tuple.get(key));
            }
            if (i < tuple.size() - 1) {
                query.append(", ");
            }
            i++;
        }

        query.append(" )");

        Statement statement;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query.toString());
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Updates a tuple in the database
     * 
     * @param table
     *            the table to update
     * @param where
     *            String after the WHERE word of a SQL query
     * @param tuple
     *            Values to update in the table
     */
    static void update(Connection connection, String table, String where,
            TreeMap<String, Object> tuple) {

        // Build columns
        StringBuilder query = new StringBuilder();
        query.append(UPDATE + " " + table);

        query.append(" " + SET + " ");
        int i = 0;
        for (String key : tuple.keySet()) {
            query.append(key);
            query.append(" = ");
            if (tuple.get(key) instanceof String) {
                query.append("'");
                query.append(tuple.get(key));
                query.append("'");
            } else {
                query.append(tuple.get(key));
            }
            if (i < tuple.size() - 1) {
                query.append(", ");
            }
            i++;
        }

        query.append(" " + WHERE + " " + where);

        Statement statement;
        try {
            statement = connection.createStatement();
            statement.executeUpdate(query.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
