package jdtx.repl.main.api.que;

/**
 * Общая очередь реплик - хранение и упорядочивание.
 */
public interface IJdxQueCommon extends IJdxReplicaStorage, IJdxQueReplica, IJdxStorageFile {

    /**
     * @return Последний возраст реплики в очереди для рабочей станции wsId
     * @deprecated
     */
    long getMaxAge(long wsId) throws Exception;

}
