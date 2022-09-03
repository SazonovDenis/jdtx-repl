package jdtx.repl.main.api.decoder;

import jandcode.app.*;
import jandcode.dbm.db.*;

//todo доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы
public abstract class RefManagerService extends CompRt {

    public abstract IRefManager createRefManager(Db db, long self_ws_id) throws Exception;

}
