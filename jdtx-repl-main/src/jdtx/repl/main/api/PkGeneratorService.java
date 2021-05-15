package jdtx.repl.main.api;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

//todo доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы
public abstract class PkGeneratorService extends CompRt {

    public abstract IPkGenerator createGenerator(Db db, IJdxDbStruct struct) throws Exception;

}
