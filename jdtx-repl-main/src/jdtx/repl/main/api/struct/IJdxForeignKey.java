package jdtx.repl.main.api.struct;

public interface IJdxForeignKey {

    /**
     * @return Название constraint-та в БД
     */
    String getName();

    /**
     * @return Поле, которое ссылается (собственно ссылочное поле)
     */
    IJdxField getField();

    /**
     * @return Таблица, на которую ссылаемся
     */
    IJdxTable getTable();

    /**
     * @return Поле в таблице, на которое ссылаемся
     */
    IJdxField getTableField();

    void setName(String name);

    void setField(IJdxField field);

    void setTable(IJdxTable table);

    void setTableField(IJdxField field);

    IJdxForeignKey cloneForeignKey();

}