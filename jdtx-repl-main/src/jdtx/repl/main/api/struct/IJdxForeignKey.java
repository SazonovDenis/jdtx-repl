package jdtx.repl.main.api.struct;

public interface IJdxForeignKey {

    /**
     * @return ѕоле, которое ссылаетс€ (собственно ссылочное поле)
     */
    IJdxFieldStruct getField();

    /**
     * @return “аблица, на которую ссылаемс€
     */
    IJdxTableStruct getTable();

    /**
     * @return ѕоле в таблице, на которое ссылаемс€
     */
    IJdxFieldStruct getTableField();

    void setField(IJdxFieldStruct field);

    void setTable(IJdxTableStruct table);

    void setTableField(IJdxFieldStruct field);

    IJdxForeignKey cloneForeignKey();

}