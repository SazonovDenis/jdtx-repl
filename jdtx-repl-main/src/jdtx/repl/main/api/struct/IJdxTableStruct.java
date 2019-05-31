package jdtx.repl.main.api.struct;

import java.util.*;

public interface IJdxTableStruct {

    String getName();

    List<IJdxFieldStruct> getFields();

    List<IJdxFieldStruct> getPrimaryKey();

    List<IJdxForeignKey> getForeignKeys();

    //IJdxTableStruct cloneTable();

    /**
     * Возвращает поле по имени
     *
     * @param tableName имя поля
     * @return поле или null, если такого поля нет
     */
    IJdxFieldStruct getField(String tableName);

}
