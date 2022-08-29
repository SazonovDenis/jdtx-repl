package jdtx.repl.main.api.util;

import jandcode.dbm.db.*;
import jandcode.utils.error.*;

public class UtDbNameManager {

////////////////
////////////////
////////////////
////////////////
// todo ksv: как правильно создавать
////////////////
////////////////
////////////////
////////////////

    private static IUtDbNameManager inst = null;

    public static IUtDbNameManager getInst(Db db) {
        if (inst == null) {
            String dbType = UtJdx.getDbType(db);
            if (dbType.equalsIgnoreCase("oracle")) {
                inst = new UtDbNameManager_Oracle();
            } else if (dbType.equalsIgnoreCase("firebird")) {
                inst = new UtDbNameManager_Firebird();
            } else {
                throw new XError("Неизвестный тип базы: " + dbType);
            }
        }
        return inst;
    }

}
