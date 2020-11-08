package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

/**
 * Хранилище реплик
 */
public interface IJdxReplicaStorage {

    /**
     * Поместить в хранилище по номеру
     */
    void put(IReplica replica, long no) throws Exception;

    /**
     * @param no Номер реплики
     * @return Реплика по номеру
     */
    IReplica get(long no) throws Exception;

}
