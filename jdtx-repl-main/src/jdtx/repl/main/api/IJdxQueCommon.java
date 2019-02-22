package jdtx.repl.main.api;

/**
 * Формирователь общей очереди реплик - хранение и упорядочивание.
 */
public interface IJdxQueCommon {

    /**
     * Поместить в очередь
     */
    long put(IReplica replica) throws Exception;

    /**
     * @return Сколько записей есть в очереди
     */
    long getMaxNo() throws Exception;

    /**
     * Получить реплику по номеру
     *
     * @param no Номер
     * @return Реплика
     */
    IReplica getByNo(long no) throws Exception;

    /**
     *
     */
    String getBaseDir();

    /**
     *
     */
    void setBaseDir(String baseDir);


}
