package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

public class UtDbStructInfo {

    private Db db;
    private IJdxDbStruct struct;

    public UtDbStructInfo(Db db, IJdxDbStruct struct) {
        this.struct = struct;
        this.db = db;
    }

    public String getDatabaseInfo() throws Exception {
        String dbStructActualCrc = UtDbComparer.getDbStructCrcTables(struct);
        IUtAppDbVersionReader utVersionReader = new UtAppDbVersionReader_PS(db); // Пока для PS
        String dbVersionActual = utVersionReader.readDbVersion();
        return dbStructActualCrc.substring(0, 8) + ":" + dbVersionActual;
    }

}
