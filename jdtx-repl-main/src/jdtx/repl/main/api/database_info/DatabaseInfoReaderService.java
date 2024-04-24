package jdtx.repl.main.api.database_info;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

public abstract class DatabaseInfoReaderService extends CompRt {

    public abstract IDatabaseInfoReader createDatabaseInfoReader(Db db, IJdxDbStruct struct) throws Exception;

}
