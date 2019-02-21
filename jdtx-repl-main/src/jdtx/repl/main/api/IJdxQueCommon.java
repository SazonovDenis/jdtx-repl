package jdtx.repl.main.api;

/**
 * Формирователь очереди реплик - хранение реплик и их упорядочивание.
 */
public interface IJdxQueCommon {

    /**
     * Поместить replica в очередь
     */
    void put(IReplica replica) throws Exception;

    /**
     * @return Сколько записей есть в очереди
     */
    long getMaxId() throws Exception;

    /**
     *
     * @param idx
     * @return
     * @throws Exception
     */
    IReplica getById(long idx) throws Exception;

}
