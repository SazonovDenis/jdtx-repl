package jdtx.repl.main.api;

/**
 * Формирователь очереди реплик - хранение реплик и их упорядочивание.
 */
public interface IJdxQueOut {

    /**
     * Поместить replica в очередь
     */
    void put(IReplica replica) throws Exception;

    /**
     * @return Реплика какого возраста есть в очереди
     */
    long getMaxAge() throws Exception;

}
