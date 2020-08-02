package jdtx.repl.main.api.struct;

/**
 * Информация о поле в таблице
 */
public interface IJdxField {

    /**
     * @return Название поля в БД
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

    // todo: IJdxTable.getPrimaryKey подразумевает несколько полей в составе PrimaryKey,
    // а тут реализовано, как будто PrimaryKey всегда одно поле. Это верно для ТБД и PS, но в общем не это так.
    boolean isPrimaryKey();

    IJdxTable getRefTable();

    IJdxField cloneField();

}
