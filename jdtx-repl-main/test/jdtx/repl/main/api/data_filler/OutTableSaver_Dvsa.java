package jdtx.repl.main.api.data_filler;

import jandcode.dbm.*;
import jandcode.dbm.data.*;

class OutTableSaver_Dvsa extends OutTableSaver {

    OutTableSaver_Dvsa(DataStore data) {
        super(data);
    }

    int maxColWidth = 25;

    void setMaxColWidth(int maxColWidth) {
        this.maxColWidth = maxColWidth;
    }

    protected String getFieldValue(DataRecord rec, Field f) {
        String v = super.getFieldValue(rec, f);

        if (v.length() > maxColWidth) {
            v = v.substring(0, maxColWidth) + "...";
        }

        return v;
    }

}

