package jdtx.repl.main.api.struct;

public class JdxForeignKey implements IJdxForeignKey {

    String name;
    IJdxField field;
    IJdxTable table;
    IJdxField tableField;

    public String getName() {
        return name;
    }

    public IJdxField getField() {
        return field;
    }

    public IJdxTable getTable() {
        return table;
    }

    public IJdxField getTableField() {
        return this.tableField;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setField(IJdxField field) {
        this.field = field;
    }

    public void setTable(IJdxTable table) {
        this.table = table;
    }

    public void setTableField(IJdxField field) {
        this.tableField = field;
    }

    public IJdxForeignKey cloneForeignKey() {
        IJdxForeignKey fk = new JdxForeignKey();
        String name = this.getName();
        IJdxField ownField = this.getField();
        IJdxTable refTable = this.getTable();
        IJdxField refField = this.getTableField();
        fk.setName(name);
        fk.setField(ownField);
        fk.setTable(refTable);
        fk.setTableField(refField);
        return fk;
    }
}
