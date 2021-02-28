package jdtx.repl.main.api.database_info;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

/**
 * "Логическая" версия базы данных.
 * Реализация для PS.
 */
public class DatabaseInfoReaderSrvice_PS extends DatabaseInfoReaderService {

    @Override
    public IDatabaseInfoReader createDatabaseInfoReader(Db db, IJdxDbStruct struct) {
        return new DatabaseInfoReader_PS(db, struct);
    }

}
