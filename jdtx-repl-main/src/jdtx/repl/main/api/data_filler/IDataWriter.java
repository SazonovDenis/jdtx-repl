package jdtx.repl.main.api.data_filler;

import java.util.*;

public interface IDataWriter {

    Map<Long, Map> ins(String tableName, int count) throws Exception;

    Map<Long, Map> ins(String tableName, int count, Map<String, Object> generators) throws Exception;


    Map<Long, DataFillerRec> upd(String tableName, int count);

    Map<Long, DataFillerRec> upd(String tableName, int count, Map<String, Object> valuesVariansSet);

    Map<Long, DataFillerRec> upd(String tableName, Collection<Long> ids);

    Map<Long, DataFillerRec> upd(String tableName, Collection<Long> ids, Map<String, Object> valuesVariansSet);


    Map<Long, DataFillerRec> del(String tableName, int count, boolean cascade) throws Exception;

    Map<Long, DataFillerRec> del(String tableName, Collection<Long> ids, boolean cascade) throws Exception;

}
