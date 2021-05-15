package jdtx.repl.main.api;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

//todo доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы
public class PkGeneratorService_PS extends PkGeneratorService {

    public IPkGenerator createGenerator(Db db, IJdxDbStruct struct) {
        return new PkGenerator_PS(db, struct);
    };

}
