package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

public interface IJdxQueReplica {

    /**
     * Поместить очередную реплику в очередь
     *
     * @return Номер реплики в очереди
     */
    long push(IReplica replica) throws Exception;

}
