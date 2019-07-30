package jdtx.repl.main.ut;

import jandcode.dbm.db.*;
import jandcode.utils.*;
import jandcode.utils.error.*;

import java.sql.*;
import java.util.*;

/**
 * Реализация direct connection
 *
 * Указать в :
 * <pre>{@code
    <dbdriver name="jdbc" parent="sys">
        <service name="jandcode.dbm.db.ConnectionService"
            class="DirectConnectionService"
        />

 * }</pre>
 */
public class DirectConnectionService extends ConnectionService {

    public Connection connect() {
        //
        DbSource dbSource = getDbSource();

        //
        try {
            String cn = dbSource.getJdbcDriverClass();
            try {
                getApp().getClass(cn);
            } catch (Exception e) {
                throw new XErrorWrap(e);
            }

            Properties props = new Properties();
            if (dbSource.hasUsername()) {
                props.put("user", dbSource.getUsername());
            }
            if (dbSource.hasPassword()) {
                props.put("password", dbSource.getPassword());
            }
            Connection conn = DriverManager.getConnection(dbSource.getUrl(), props);

            String db = dbSource.getDatabase();
            if (!UtString.empty(db)) {
                conn.setCatalog(db);
            }

            return conn;
        } catch (Exception e) {
            throw new XErrorWrap(e);
        }
    }

    public void disconnect(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new XErrorWrap(e);
            }
        }
    }

    public void disconnectAll() {
    }

}
