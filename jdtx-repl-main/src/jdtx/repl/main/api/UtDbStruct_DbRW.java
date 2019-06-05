package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;

import java.io.*;

public class UtDbStruct_DbRW {

    private Db db;

    public UtDbStruct_DbRW(Db db) {
        this.db = db;
    }

    public IJdxDbStruct getDbStructAllowed() throws Exception {
        return getDbStructInternal("db_struct_allowed");
    }


    public IJdxDbStruct getDbStructFixed() throws Exception {
        return getDbStructInternal("db_struct_fixed");
    }

    IJdxDbStruct getDbStructInternal(String name) throws Exception {
        DataStore st = db.loadSql("select " + name + " from Z_Z_state where id = 1");
        byte[] db_struct = (byte[]) st.getCurRec().getValue(name);
        //
        if (db_struct.length == 0) {
            return null;
        }
        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        return struct_rw.read(db_struct);
    }

    public void dbStructSaveAllowed(IJdxDbStruct struct) throws Exception {
        dbStructSaveInternal(struct, "db_struct_allowed");
    }

    public void dbStructSaveFixed(IJdxDbStruct struct) throws Exception {
        dbStructSaveInternal(struct, "db_struct_fixed");
    }

    void dbStructSaveInternal(IJdxDbStruct struct, String name) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        byte[] bytes = struct_rw.getBytes(struct);
        db.execSql("update Z_Z_state set " + name + " = :struct where id = 1", UtCnv.toMap("struct", bytes));
    }

}
