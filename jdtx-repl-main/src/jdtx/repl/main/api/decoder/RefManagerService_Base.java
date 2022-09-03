package jdtx.repl.main.api.decoder;

import jandcode.dbm.db.*;

public class RefManagerService_Base extends RefManagerService {

    @Override
    public IRefManager createRefManager(Db db, long self_ws_id) throws Exception {
        return new RefManagerBase();
    }

}
