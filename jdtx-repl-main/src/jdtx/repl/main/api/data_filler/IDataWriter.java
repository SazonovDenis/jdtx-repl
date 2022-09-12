package jdtx.repl.main.api.data_filler;

import java.util.*;

public interface IDataWriter {

    Map<Long, Map<String, Object>> ins(String tableName, int count) throws Exception;

    Map<Long, Map<String, Object>> ins(String tableName, int count, Map<String, Object> tableGenerators) throws Exception;


    Map<Long, Map<String, Object>> upd(String tableName, int count) throws Exception;

    Map<Long, Map<String, Object>> upd(String tableName, int count, Map<String, Object> tableGenerators) throws Exception;

    Map<Long, Map<String, Object>> upd(String tableName, Collection<Long> ids) throws Exception;

    Map<Long, Map<String, Object>> upd(String tableName, Collection<Long> ids, Map<String, Object> tableGenerators) throws Exception;


    void del(String tableName, int count, boolean cascade) throws Exception;

    void del(String tableName, Collection<Long> ids, boolean cascade) throws Exception;

}
