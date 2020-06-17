package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import java.util.*;

class UtRecDuplicate {
    Map params;
    DataStore records;
}

class UtRecMergeTask {
    String tableName;
    DataRecord recordEtalon;
    Collection<Long> recordsDelete;
}

class UtRemoveDuplicatesRes {
    String tableName;
    Collection<DataRecord> recordsUpdated;
}