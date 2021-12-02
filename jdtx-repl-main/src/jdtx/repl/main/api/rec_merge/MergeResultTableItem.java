package jdtx.repl.main.api.rec_merge;

public class MergeResultTableItem {

    String tableName;
    MergeOprType tableOperation;
    String info = null;

    public MergeResultTableItem(String tableName, MergeOprType tableOperation, String info) {
        this.tableName = tableName;
        this.tableOperation = tableOperation;
        this.info = info;
    }

    public MergeResultTableItem(String tableName, MergeOprType tableOperation) {
        this.tableName = tableName;
        this.tableOperation = tableOperation;
    }

}
