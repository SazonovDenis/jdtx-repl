package jdtx.repl.main.api.que;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.replica.*;

public class JdxQueSnapshot extends JdxQueCommonFile {

    public JdxQueSnapshot(Db db, String queName) {
        super(db, queName);
    }

    @Override
    void validateReplica(IReplica replica) throws Exception {
        // Проверки: правильность полей реплики
        JdxUtils.validateReplicaFields(replica);
    }

    @Override
    public long getMaxNo() throws Exception {
        return super.getMaxNoFromDir();
    }

}
