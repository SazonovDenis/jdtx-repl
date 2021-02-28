package jdtx.repl.main.api.manager;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.apache.commons.logging.*;

/**
 * Сохранение данных о разрешенной/фиксированной структуре для реплицируемой БД (в таблице Z_Z_state)
 */
public class DatabaseStructManager {

    //
    private Db db;

    //
    private static Log log = LogFactory.getLog("jdtx.UtDbStructMarker");

    public DatabaseStructManager(Db db) {
        this.db = db;
    }

    public IJdxDbStruct getDbStructAllowed() throws Exception {
        return getDbStructInternal("db_struct_allowed");
    }

    public IJdxDbStruct getDbStructFixed() throws Exception {
        return getDbStructInternal("db_struct_fixed");
    }

    public void setDbStructAllowed(IJdxDbStruct struct) throws Exception {
        setDbStructInternal(struct, "db_struct_allowed");
    }

    public void setDbStructFixed(IJdxDbStruct struct) throws Exception {
        setDbStructInternal(struct, "db_struct_fixed");
    }

    private IJdxDbStruct getDbStructInternal(String structName) throws Exception {
        DataStore st = db.loadSql("select " + structName + " from " + UtJdx.SYS_TABLE_PREFIX + "workstation");

        //
        byte[] structBytes = (byte[]) st.getCurRec().getValue(structName);
        if (structBytes.length == 0) {
            return null;
        }

        //
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        IJdxDbStruct struct = struct_rw.read(structBytes);

        //
        return struct;
    }

    private void setDbStructInternal(IJdxDbStruct struct, String structCode) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        byte[] bytes = struct_rw.getBytes(struct);
        db.execSql("update " + UtJdx.SYS_TABLE_PREFIX + "workstation set " + structCode + " = :struct", UtCnv.toMap("struct", bytes));
        //
        log.info("setDbStruct: " + structCode);
    }

}
