package jdtx.repl.main.api.util;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;

public class UtDbErrors {


    private static IUtDbErrors inst = null;

    public static IUtDbErrors getInst(Db db) {
        if (inst == null) {
            String dbType = UtJdx.getDbType(db);
            if (dbType.equalsIgnoreCase("oracle")) {
                inst = new UtDbErrors_Oracle();
            } else if (dbType.equalsIgnoreCase("firebird")) {
                inst = new UtDbErrors_Firebird();
            } else {
                throw new XError("Неизвестный тип базы: " + dbType);
            }
        }
        return inst;
    }

}
