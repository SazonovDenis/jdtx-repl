package jdtx.repl.main.api.ref_manager;

import jandcode.utils.error.*;
import jdtx.repl.main.api.data_serializer.*;
import jdtx.repl.main.api.struct.*;

/**
 * Реализация IRefManager, если разведение pk не нужно.
 */
public class RefManager_Base extends RefManagerService implements IRefManager {


    // ------------------------------------------
    // RefManagerService
    // ------------------------------------------

    @Override
    public IJdxDataSerializer createDataSerializer() {
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

    @Override
    public long get_max_own_id(IJdxTable table) throws Exception {
        throw new XError("Not implemented");
    }

    @Override
    public boolean isPresent_not_own_id(IJdxTable table) throws Exception {
        throw new XError("Not implemented");
    }


}
