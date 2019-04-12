package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

/**
 * Формирователь очереди собственных реплик - хранение и упорядочивание.
 */
public interface IJdxQuePersonal {

    /**
     * Поместить в очередь
     */
    void put(IReplica replica) throws Exception;

    /**
     * @return Реплика какого возраста есть в очереди
     */
    long getMaxAge() throws Exception;

    /**
     * Получить реплику по номеру (возрасту)
     *
     * @param age Номер (возраст)
     * @return Реплика
     */
    IReplica getByAge(long age) throws Exception;

    /**
     * todo: Это только для реплик в файлах. А вообще может быть по-другому.
     */
    String getBaseDir();

    /**
     * todo: Это только для реплик в файлах. А вообще может быть по-другому.
     */
    void setBaseDir(String baseDir);

}
