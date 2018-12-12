package jdtx.repl.main.api.struct;

public class JdxForeignKey implements IJdxForeignKey {

    IJdxFieldStruct field;
    IJdxTableStruct table;
    IJdxFieldStruct tableField;

    public IJdxFieldStruct getField() {
        return field;
    }

    public IJdxTableStruct getTable() {
        return table;
    }

    public IJdxFieldStruct getTableField() {
        return this.tableField;
    }

    public void setField(IJdxFieldStruct field) {
        this.field = field;
    }

    public void setTable(IJdxTableStruct table) {
        this.table = table;
    }

    public void setTableField(IJdxFieldStruct field) {
        this.tableField = field;
    }

    public IJdxForeignKey cloneForeignKey() {
        IJdxForeignKey fk = new JdxForeignKey();
        IJdxFieldStruct ownField = this.getField();
        IJdxTableStruct refTable = this.getTable();
        IJdxFieldStruct refField = this.getTableField();
        fk.setField(ownField);
        fk.setTable(refTable);
        fk.setTableField(refField);
        return fk;
    }
}
