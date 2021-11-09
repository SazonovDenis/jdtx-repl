package jdtx.repl.main.api.rec_merge;

public class MergeResultTableItem {

    public static final int UPD = 1;
    public static final int DEL = 2;

    String tableName;
    int tableOperation;

    public MergeResultTableItem(String tableName, int tableOperation) {
        this.tableName = tableName;
        this.tableOperation = tableOperation;
    }

}
