package com.boosteel.nativedb;


import com.boosteel.nativedb.core.RepositoryFactory;
import com.boosteel.nativedb.core.SQL;
import com.boosteel.nativedb.core.TableInfo;
import com.zaxxer.hikari.HikariConfig;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class NativeDB {


    public static DataSource createDataSource(String url, String user, String password) {
        return createDataSource(url, user, password, "org.mariadb.jdbc.Driver");
    }

    public static DataSource createDataSource(String url, String user, String password, String className) {
        HikariConfig config = new HikariConfig();
        config.setDataSourceClassName(className);
        config.setPassword(password);
        config.setUsername(user);
        config.setJdbcUrl(url);
        return config.getDataSource();
    }

    public DataSource dataSource;
    private String url;
    private String user;
    private String password;

    public NativeDB(String url, String user, String password) {
        this(createDataSource(url, user, password));
    }

    public NativeDB(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    public<T> T createRepository(Class<T> clazz) {
        return RepositoryFactory.createRepository(clazz, this);
    }

    public void doStmt(DoStatement doWork) {
        doStmt(doWork, false);
    }

    public void doStmt(DoStatement doWork, boolean autoCommit) {
        transaction((con) -> {
            try (Statement s = con.createStatement()) {
                doWork.accept(s);
            }
            return null;
        }, autoCommit);
    }

    public <T> T doStmtR(DoStatementR<T> doWork) {
        return doStmtR(doWork, false);
    }
    public <T> T doStmtR(DoStatementR<T> doWork, boolean autoCommit) {
        return transaction((con) -> {
            try (Statement s = con.createStatement()) {
                return doWork.accept(s);
            }
        }, autoCommit);
    }

    public void doWork(DoWork doWork) {
        this.doWork(doWork, false);
    }
    public void doWork(DoWork doWork, boolean autoCommit) {

        transaction((con) -> {
            doWork.accept(con);
            return null;
        }, autoCommit);
    }

    public <T> T doWorkR(DoWorkR<T> doWork) {
        return this.doWorkR(doWork, false);
    }
    public <T> T doWorkR(DoWorkR<T> doWork, boolean autoCommit) {
        return transaction((con) -> doWork.accept(con), autoCommit);
    }

    public <T> T execute(String sql, ACCEPT_RS<T> e) {
        try (Connection con = getCon()) {
            return execute(con, sql, e);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public void execute(String sql, ACCEPT e) {
        try (Connection con = getCon()) {
            execute(con, sql, e);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public void execute(Connection con, String sql, ACCEPT e) {
        try (Statement s = con.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            e.execute(rs);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public <T> T execute(Connection con, String sql, ACCEPT_RS<T> e) {
        try (Statement s = con.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return e.execute(rs);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }


    // 여러가지 값을 가지고 올때
    public <T> List<T> execute(String sql, List<T> list, EXECUTE_RETURN<T> e) {
        try (Connection con = getCon()) {
            return execute(con, sql, list, e);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public <T> List<T> execute(Connection con, String sql, List<T> list, EXECUTE_RETURN<T> e) {
        try (Statement s = con.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            int len = 0;
            while (rs.next())
                list.add(e.execute(rs, len++));
            return list;
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    // 하나의 값만 가지고 올때
    public <T> T execute(String sql, T defaultValue, ACCEPT_RS<T> e) {
        try (Connection con = getCon()) {
            return execute(con, sql, defaultValue, e);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public <T> T execute(Connection con, String sql, T defaultValue, ACCEPT_RS<T> e) {
        try (Statement s = con.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? e.execute(rs) : defaultValue;
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }



    public TableInfo getTableInfo(String tableName) {
        return TableInfo.create(tableName, this);
    }

    public int update(String sql) throws SQLException {
        try (Connection con = getCon();
             Statement s = con.createStatement();) {
            return s.executeUpdate(sql);
        }
    }

    public long save(String sql) {
        return save(sql, false);
    }

    public long save(String sql, boolean last_id) {
        try (Connection con = getCon();
             Statement s = con.createStatement()) {
            return save(s, sql, last_id);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public long save(Statement s, String sql, boolean last_id) {
        try {
            long i = s.executeUpdate(sql);
            if (last_id) {
                try (ResultSet rs = s.executeQuery("SELECT LAST_INSERT_ID()")) {
                    rs.next();
                    i = rs.getLong(1);
                }
            }
            return i;
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public long save(String tableName, List<Map<String, Object>> values) {
        String sql = SQL.insert(tableName, values);
        try (Connection con = getCon();
             Statement s = con.createStatement()) {
            return s.executeUpdate(sql);
        } catch (Exception a) {
            throw new RuntimeException(a);
        }
    }

    public Connection getCon() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T transaction(DoWorkR<T> d, boolean autoCommit) {
        try (Connection con = getCon()) {

            if (autoCommit) {
                con.setAutoCommit(true);
                return d.accept(con);
            } else {
                try {
                    con.setAutoCommit(false);
                    T r = d.accept(con);
                    con.commit();
                    return r;
                } catch (Exception e) {
                    con.rollback();
                    throw e;
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public long getLong(Statement s, String sql) throws SQLException {
        ResultSet rs = s.executeQuery(sql);
        return rs.next() ? rs.getLong(1) : null;
    }

    public int getInt(Statement s, String sql) throws SQLException {
        ResultSet rs = s.executeQuery(sql);
        return rs.next() ? rs.getInt(1) : null;
    }

    public String getString(Statement s, String sql) throws SQLException {
        ResultSet rs = s.executeQuery(sql);
        return rs.next() ? rs.getString(1) : null;
    }

    public Date getDate(Statement s, String sql) throws SQLException {
        ResultSet rs = s.executeQuery(sql);
        return rs.next() ? new Date(rs.getTimestamp(1).getTime()) : null;
    }


    public interface ThreadRun {
        void run() throws Exception;
    }

    public interface ACCEPT {
        void execute(ResultSet rs) throws Exception;
    }

    public interface ACCEPT_RS<T> {
        T execute(ResultSet rs) throws Exception;
    }

    public interface EXECUTE {
        void execute(ResultSet rs, int index) throws Exception;
    }

    public interface EXECUTE_RETURN<T> {
        T execute(ResultSet rs, int index) throws Exception;
    }

    public interface DoWork {
        void accept(Connection con) throws Exception;
    }


    public interface DoWorkR<T> {
        T accept(Connection con) throws Exception;
    }

    public interface DoStatement {
        void accept(Statement s) throws Exception;
    }

    public interface DoStatementR<T> {
        T accept(Statement s) throws Exception;
    }

    public interface DoPrepare {
        void accept(PreparedStatement s) throws Exception;
    }

    public interface DoPrepareR<T> {
        T accept(PreparedStatement s) throws Exception;
    }
}
