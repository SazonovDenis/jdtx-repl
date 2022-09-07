package jdtx.repl.main.api.pk_generator;

/**
 * Управление последовательностями СУБД
 */
public interface IDbGenerators {

    long getGeneratorCurrValue(String generatorName) throws Exception;

    long getGeneratorNextValue(String generatorName) throws Exception;

    void setGeneratorValue(String generatorName, long value) throws Exception;

    void createGenerator(String generatorName) throws Exception;

    void dropGenerator(String generatorName) throws Exception;

}
