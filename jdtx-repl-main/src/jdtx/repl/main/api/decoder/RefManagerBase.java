package jdtx.repl.main.api.decoder;

import jandcode.dbm.db.*;

/**
 * Реализация IRefManager, если разведение pk не нужно.
 */
public class RefManagerBase implements IRefManager {

    public RefManagerBase() {
    }

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
