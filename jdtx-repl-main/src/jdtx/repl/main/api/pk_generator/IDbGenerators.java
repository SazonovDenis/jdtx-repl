package jdtx.repl.main.api.pk_generator;

/**
 * Управление последовательностями СУБД
 */
public interface IDbGenerators {

    /**
     * Получить следующее значение генератора (последовательности)
     *
     * @param generatorName для какого генератора
     */
    long genNextValue(String generatorName) throws Exception;

    /**
     * Получить последнее значение генератора (последовательности)
     *
     * @param generatorName для какого генератора
     * @return значение, которое было выдано последним вызовом {@link jdtx.repl.main.api.pk_generator.IDbGenerators#genNextValue(java.lang.String)}
     */
    long getLastValue(String generatorName) throws Exception;

    /**
     * Задать последнее значение генератора (последовательности).
     * Следующий вызов {@link jdtx.repl.main.api.pk_generator.IDbGenerators#genNextValue(java.lang.String)} выдаст другое значение.
     *
     * @param generatorName для какого генератора
     */
    void setLastValue(String generatorName, long value) throws Exception;

    void createGenerator(String generatorName) throws Exception;

    void dropGenerator(String generatorName) throws Exception;

}
