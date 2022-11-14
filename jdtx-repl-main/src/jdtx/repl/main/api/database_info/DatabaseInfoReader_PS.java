package jdtx.repl.main.api.database_info;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;

/**
 * "Логическая" версия базы данных, реализация для PS.
 */
public class DatabaseInfoReader_PS implements IDatabaseInfoReader {

    private Db db;
    private IJdxDbStruct struct;

    public DatabaseInfoReader_PS(Db db, IJdxDbStruct struct) {
        this.struct = struct;
        this.db = db;
    }

    @Override
    public String readDatabaseVersion() throws Exception {
        String dbStructActualCrc = UtDbComparer.getDbStructCrcTables(struct);
        //
        DataRecord rec = db.loadSql("select * from VER_DB where ID = 1").getCurRec();
        String dbVersionActual = rec.getValueString("ver") + "." + rec.getValueString("ver_step");
        //
        return dbStructActualCrc.substring(0, 8) + ":" + dbVersionActual;
    }

}
