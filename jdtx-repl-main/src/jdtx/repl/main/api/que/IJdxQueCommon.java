package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

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
     * @return Последний возраст реплики в очереди для рабочей станции wsId
     */
    long getMaxAge(long wsId) throws Exception;


    /**
     * Получить реплику по номеру
     *
     * @param no Номер
     * @return Реплика
     */
    IReplica getByNo(long no) throws Exception;

    /**
     * todo: Это только для реплик в файлах. А вообще может быть по-другому.
     */
    String getBaseDir();

    /**
     * todo: Это только для реплик в файлах. А вообще может быть по-другому.
     */
    void setBaseDir(String baseDir);


}
