package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

public interface IJdxReplicaStorage {

    /**
     * Поместить в хранилище по номеру
     */
    void put(IReplica replica, long no) throws Exception;

    /**
     * Получить реплику по номеру
     *
     * @param no Номер
     * @return Реплика
     */
    IReplica get(long no) throws Exception;

    /**
     * @return До какого номера есть реплики в хранилище
     */
    long getMaxNo() throws Exception;

}
