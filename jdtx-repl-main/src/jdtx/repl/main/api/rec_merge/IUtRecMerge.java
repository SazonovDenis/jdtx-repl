package jdtx.repl.main.api.rec_merge;

import java.util.*;

public interface IUtRecMerge {

    Collection<String> loadTables();

    Collection<String> loadTableFields(String tableName);

    Collection<UtRecDuplicate> loadTableDuplicates(String tableName, String[] fieldNames) throws Exception;

    Collection<UtRemoveDuplicatesRes> execRemoveDuplicates(Collection<UtRecMergeTask> tasks) throws Exception;

}
