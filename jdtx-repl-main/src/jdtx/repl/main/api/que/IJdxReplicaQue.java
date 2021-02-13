package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

/**
 * Очередь реплик
 */
public interface IJdxReplicaQue {


    /**
     * Поместить очередную реплику в очередь
     *
     * @return Номер реплики в очереди
     */
    long push(IReplica replica) throws Exception;

    /**
     * Проверка правильности реплики перед помещением в очередь
     */
    void validateReplica(IReplica replica) throws Exception;

    /**
     * @return До какого номера есть реплики в хранилище
     */
    long getMaxNo() throws Exception;

    /**
     * Установить счетчик номеров
     *
     * @param maxNo Значение счетчика номеров
     */
    void setMaxNo(long maxNo) throws Exception;


}
