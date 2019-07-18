package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxTable {

    String getName();

    List<IJdxField> getFields();

    List<IJdxField> getPrimaryKey();

    List<IJdxForeignKey> getForeignKeys();

    /**
     * Возвращает поле по имени
     *
     * @param tableName имя поля
     * @return поле или null, если такого поля нет
     */
    IJdxField getField(String tableName);

}
