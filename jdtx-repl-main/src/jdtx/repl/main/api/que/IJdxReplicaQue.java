package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

/**
 * Очередь реплик
 */
public interface IJdxReplicaQue {


    /**
     * Проверка правильности реплики перед помещением в очередь
     */
    void validateReplica(IReplica replica) throws Exception;

    /**
     * Поместить очередную реплику в очередь
     *
     * @return Номер реплики в очереди
     */
    long push(IReplica replica) throws Exception;

    /**
     * @return До какого номера есть реплики в очереди
     */
    long getMaxNo() throws Exception;

    /**
     * @return От какого номера есть реплики в очереди
     */
    long getMinNo() throws Exception;

    /**
     * Установить счетчик номеров реплик.
     * Нужен для восстановлении состояния при ремонте рабочей станции (или при добавлении новой станции)
     *
     * @param maxNo Значение счетчика номеров
     */
    void setMaxNo(long maxNo) throws Exception;


}
