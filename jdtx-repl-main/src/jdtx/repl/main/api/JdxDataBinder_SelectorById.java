package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jandcode.dbm.db.*;

import java.util.*;

public class JdxDataBinder_SelectorById implements IJdxDataBinder {

    private String tableName;
    private String tableFields;
    private Collection<Long> idList;
    private int pos;

    private Db db;
    private Iterator<Long> iterator;
    private DataRecord record;

    public JdxDataBinder_SelectorById(Db db, String tableName, String tableFields, Collection<Long> idList) throws Exception {
        this.tableName = tableName;
        this.tableFields = tableFields;
        this.idList = idList;
        this.db = db;
        iterator = idList.iterator();
        //
        pos = -1;
        next();
    }

    @Override
    public Object getValue(String name) {
        return record.getValue(name);
    }

    @Override
    public void next() throws Exception {
        if (!eof()) {
            if (iterator.hasNext()) {
                // todo: Прямое использование id заменить на корректное, по структуре
                record = db.loadSql("select " + tableFields + " from " + tableName + " where id = " + iterator.next()).getCurRec();
            }
            pos = pos + 1;
        }
    }

    @Override
    public boolean eof() throws Exception {
        return pos >= idList.size();
    }

    @Override
    public void close() throws Exception {

    }

}
