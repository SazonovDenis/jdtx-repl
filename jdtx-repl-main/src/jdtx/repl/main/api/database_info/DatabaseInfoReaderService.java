package jdtx.repl.main.api.database_info;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.decoder.*;
import jdtx.repl.main.api.struct.*;

// todo доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы
public abstract class DatabaseInfoReaderService extends CompRt {

    public abstract IDatabaseInfoReader createDatabaseInfoReader(Db db, IJdxDbStruct struct) throws Exception;

}
