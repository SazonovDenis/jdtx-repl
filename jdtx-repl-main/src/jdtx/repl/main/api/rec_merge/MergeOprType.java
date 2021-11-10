package jdtx.repl.main.api.rec_merge;

public enum MergeOprType {

    UPD(1),
    DEL(2);

    int value;

    MergeOprType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}