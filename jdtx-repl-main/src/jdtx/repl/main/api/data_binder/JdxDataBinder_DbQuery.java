package jdtx.repl.main.api.data_binder;

import jandcode.dbm.db.*;
import jandcode.utils.*;

import java.util.*;

public class JdxDataBinder_DbQuery implements IJdxDataBinder {

    private DbQuery query;

    public JdxDataBinder_DbQuery(DbQuery query) {
        this.query = query;
    }

    @Override
    public Map<String, Object> getValues() {
        HashMapNoCase<Object> res = new HashMapNoCase<>();
        res.putAll(query.getValues());
        return res;
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
