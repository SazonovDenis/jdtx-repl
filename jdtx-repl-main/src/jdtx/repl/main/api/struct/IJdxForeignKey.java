package jdtx.repl.main.api.struct;

public interface IJdxForeignKey {

    /**
     * @return Название в БД
     */
    String getName();

    /**
     * @return Поле, которое ссылается (собственно ссылочное поле)
     */
    IJdxFieldStruct getField();

    /**
     * @return Таблица, на которую ссылаемся
     */
    IJdxTableStruct getTable();

    /**
     * @return Поле в таблице, на которое ссылаемся
     */
    IJdxFieldStruct getTableField();

    void setName(String name);

    void setField(IJdxFieldStruct field);

    void setTable(IJdxTableStruct table);

    void setTableField(IJdxFieldStruct field);

    IJdxForeignKey cloneForeignKey();

}