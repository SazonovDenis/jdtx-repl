package jdtx.repl.main.api;

/**
 * Формирователь очереди реплик - хранение реплик и их упорядочивание.
 */
public interface IJdxQue {

    /**
     * Поместить replica в очередь
     */
    void put(IReplica replica) throws Exception;

    /**
     * @return Сколько записей есть в очереди
     */
    long getMaxId() throws Exception;

    //IReplica get(long idx) throws Exception;

    //long getQueMaxAge() throws Exception;

    //long getDone() throws Exception;

    //void setDone(long inIdxDone) throws Exception;

}
