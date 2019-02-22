package jdtx.repl.main.api;

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
     *
     */
    String getBaseDir();

    /**
     *
     */
    void setBaseDir(String baseDir);

}
