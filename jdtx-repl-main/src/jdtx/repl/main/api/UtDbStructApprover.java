package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

/**
 * Сохранение данных о разрешенной/фиксированной структуре для реплицируемой БД (в таблице Z_Z_state)
 */
public class UtDbStructApprover {

    private Db db;

    public UtDbStructApprover(Db db) {
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

    private IJdxDbStruct getDbStructInternal(String structCode) throws Exception {
        DataStore st = db.loadSql("select " + structCode + " from Z_Z_workstation");
        byte[] db_struct = (byte[]) st.getCurRec().getValue(structCode);
        //
        if (db_struct.length == 0) {
            return null;
        }
        //
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        return struct_rw.read(db_struct);
    }

    private void setDbStructInternal(IJdxDbStruct struct, String structCode) throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        byte[] bytes = struct_rw.getBytes(struct);
        db.execSql("update Z_Z_workstation set " + structCode + " = :struct", UtCnv.toMap("struct", bytes));
    }

}
