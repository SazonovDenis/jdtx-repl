package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;

/**
 * "Логическая" версия базы данных для PS.
 */
public class UtAppDbVersionReader_PS implements IUtAppDbVersionReader {

    private Db db;

    public UtAppDbVersionReader_PS(Db db) {
        this.db = db;
    }

    @Override
    public String readDbVersion() throws Exception {
        DataRecord rec = db.loadSql("select * from VER_DB where ID = 1").getCurRec();
        return rec.getValueString("ver") + "." + rec.getValueString("ver_step");
    }

}
