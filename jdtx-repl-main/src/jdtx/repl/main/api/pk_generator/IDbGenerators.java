package jdtx.repl.main.api.pk_generator;

/**
 * Управление последовательностями СУБД
 */
public interface IDbGenerators {

    long getNextValue(String generatorName) throws Exception;

    long getValue(String generatorName) throws Exception;

    void setValue(String generatorName, long value) throws Exception;

    void createGenerator(String generatorName) throws Exception;

    void dropGenerator(String generatorName) throws Exception;

}
