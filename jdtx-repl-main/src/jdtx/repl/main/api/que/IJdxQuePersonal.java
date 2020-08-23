package jdtx.repl.main.api.que;

import jdtx.repl.main.api.replica.*;

/**
 * Очередь собственных реплик - хранение и упорядочивание.
 */
public interface IJdxQuePersonal extends /*IJdxReplicaStorage,*/ IJdxQueReplica, IJdxStorageFile {

    /**
     * Получить реплику по возрасту
     *
     * @param age Возраст
     * @return Реплика возраста age
     */
    IReplica getByAge(long age) throws Exception;

    /**
     * @return Реплика какого возраста есть в очереди
     */
    long getMaxAge() throws Exception;

    /**
     * Получить информацию о реплике
     *
     * @param age Номер (возраст)
     * @return Информация о реплике
     */
    IReplicaInfo getInfoByAge(long age) throws Exception;

}
