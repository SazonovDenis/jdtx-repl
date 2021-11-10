package jdtx.repl.main.api.rec_merge;

public class MergeResultTableItem {

    String tableName;
    MergeOprType tableOperation;

    public MergeResultTableItem(String tableName, MergeOprType tableOperation) {
        this.tableName = tableName;
        this.tableOperation = tableOperation;
    }

}
