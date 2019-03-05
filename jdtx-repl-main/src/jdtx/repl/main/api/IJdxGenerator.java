package jdtx.repl.main.api;

import java.sql.*;

/**
 * Генератор, которым надо пользоваться при вставке записей при применении
 * реплик в схеме с перекодировкой
 */
public interface IJdxGenerator {

    /**
     * @param generatorName имя генератора
     * @return очередная id
     */
    long genId(String generatorName) throws SQLException;

}
