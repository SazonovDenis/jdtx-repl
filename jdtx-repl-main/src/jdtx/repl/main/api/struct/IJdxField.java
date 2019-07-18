package jdtx.repl.main.api.struct;

public interface IJdxField {

    /**
     * @return Название в БД
     */
    String getName();

    /**
     * @return Тип поля в БД
     */
    String getDbDatatype();

    /**
     * @return Тип поля из числа JdxDataType
     */
    JdxDataType getJdxDatatype();

    int getSize();

    boolean isPrimaryKey();

    IJdxTable getRefTable();

    IJdxField cloneField();

}
