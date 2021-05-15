package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;

public interface IPkGenerator {

    String getGeneratorName(String tableName);

    long getValue(String generatorName) throws Exception;

    void setValue(String generatorName, long value) throws Exception;

    long getMaxPk(String tableName) throws Exception;

    void repairGenerator(IJdxTable table) throws Exception;

}
