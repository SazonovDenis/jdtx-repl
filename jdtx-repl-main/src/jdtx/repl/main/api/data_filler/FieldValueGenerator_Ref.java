package jdtx.repl.main.api.data_filler;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.struct.*;

import java.util.*;

class FieldValueGenerator_Ref extends FieldValueGenerator {

    Db db;
    IJdxDbStruct struct;
    IDataFiller filler;
    UtFiller utFiller;

    public FieldValueGenerator_Ref(Db db, IJdxDbStruct struct, IDataFiller filler) {
        this.db = db;
        this.struct = struct;
        this.filler = filler;
        this.utFiller = new UtFiller(db, struct);
    }


    @Override
    public Object genValue(IJdxField field) throws Exception {
        String keyField = filler.getFieldKey(field);
        Set<Long> refValues = filler.getRefValuesCache().get(keyField);

        //
        if (refValues == null) {
            // Подготовим набор значений для ссылочных полей
            refValues = utFiller.loadAllIds(field.getRefTable());
            // Закэшируем набор значений - повторно ссылки искать - дорого
            filler.getRefValuesCache().put(keyField, refValues);
        }

        //
        return utFiller.selectOneObject(refValues);
    }

}
