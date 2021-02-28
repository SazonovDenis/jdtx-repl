package jdtx.repl.main.api.data_binder;

import jandcode.dbm.db.*;

public class JdxDataBinder_DbQuery implements IJdxDataBinder {

    private DbQuery query;

    public JdxDataBinder_DbQuery(DbQuery query) {
        this.query = query;
    }

    @Override
    public Object getValue(String name) {
        return query.getValue(name);
    }

    @Override
    public void next() throws Exception {
        query.next();
    }

    @Override
    public boolean eof() throws Exception {
        return query.eof();
    }

    @Override
    public void close() throws Exception {
        query.close();
    }

}
