package jdtx.repl.main.api.decoder;

import jandcode.app.*;
import jandcode.dbm.db.*;

//todo доделать ИСПОЛЬЗОВАНИЕ именно через СЕРВИС, убрать все конструкторы
public abstract class JdxRefDecoderService extends CompRt {

    public abstract IRefDecoder createRefDecoder(Db db, long self_ws_id) throws Exception;

}
