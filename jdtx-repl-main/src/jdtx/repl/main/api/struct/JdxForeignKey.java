package jdtx.repl.main.api.struct;

public class JdxForeignKey implements IJdxForeignKey {

    String name;
    IJdxFieldStruct field;
    IJdxTableStruct table;
    IJdxFieldStruct tableField;

    public String getName() {
        return name;
    }

    public IJdxFieldStruct getField() {
        return field;
    }

    public IJdxTableStruct getTable() {
        return table;
    }

    public IJdxFieldStruct getTableField() {
        return this.tableField;
    }

    public void setName(String name) {
        this.name = name;
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
        String name = this.getName();
        IJdxFieldStruct ownField = this.getField();
        IJdxTableStruct refTable = this.getTable();
        IJdxFieldStruct refField = this.getTableField();
        fk.setName(name);
        fk.setField(ownField);
        fk.setTable(refTable);
        fk.setTableField(refField);
        return fk;
    }
}
