package jdtx.repl.main.api.ref_manager;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;

public abstract class RefManagerService extends CompRt implements IRefManager {

    JdxReplWs ws = null;
    Db db = null;

    public void init(Db db, JdxReplWs ws) throws Exception {
        this.ws = ws;
        this.db = db;
    }

    public abstract IJdxDataSerializer getJdxDataSerializer() throws Exception;

}
