package jdtx.repl.main.api.ref_manager;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.data_serializer.*;

/**
 * Реализация IRefManager, если разведение pk не нужно.
 */
public class RefManagerBase extends RefManagerService implements IRefManager {


    // ------------------------------------------
    // RefManagerService
    // ------------------------------------------

    @Override
    public void init(Db db, JdxReplWs ws) throws Exception {
        super.init(db, ws);
    }

    @Override
    public IJdxDataSerializer getJdxDataSerializer() {
        return new JdxDataSerializerPlain();
    }


    // ------------------------------------------
    // IRefManager
    // ------------------------------------------

    @Override
    public long get_id_local(String tableName, JdxRef ref) throws Exception {
        return ref.value;
    }

    @Override
    public JdxRef get_ref(String tableName, long id_local) throws Exception {
        JdxRef ref = new JdxRef();

        ref.value = id_local;

        return ref;
    }


}
