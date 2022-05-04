package jdtx.repl.main.api.que;

import java.util.*;

/**
 * Общая очередь реплик.
 * Формируется из набора входящих очередей от рабочих станций.
 */
public interface IJdxQueCommon extends IJdxReplicaStorage, IJdxReplicaQue {

    /**
     * Задать набор входящих очередей рабочих станций.
     */
    void setSrvQueIn(Map<Long, IJdxQue> srvQueInList);

    /**
     * Найти номер реплики в очереди queCommon
     * по номеру реплики в исходящей очереди автора этой реплики
     *
     * @param authorNo номер реплики в исходящей очереди автора
     * @param wsId     станция - автор реплики
     * @return Номер искомой реплики в очереди queCommon
     */
    long getNoByAuthorNo(long authorNo, long wsId) throws Exception;

}
