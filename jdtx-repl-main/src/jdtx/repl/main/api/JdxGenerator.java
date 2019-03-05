package jdtx.repl.main.api;


import jandcode.dbm.db.*;

import java.sql.*;

public class JdxGenerator implements IJdxGenerator {


    Db db;

    public JdxGenerator(Db db) {
        this.db = db;
    }

    /**
     * Дергает хранимую процедуру JdxGenID, которая возвращает очередную id для генератора generatorName
     */
    public long genId(String generatorName) throws SQLException {
        Statement st = db.getConnection().createStatement();
        ResultSet rs = null;
        try {
            rs = st.executeQuery("execute procedure " + JdxUtils.audit_table_prefix + "GenID('" + generatorName + "')");
            if (rs.next()) {
                return rs.getLong("id");
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Exception ex) {
            }
            st.close();
        }
        return 0;
    }

}
