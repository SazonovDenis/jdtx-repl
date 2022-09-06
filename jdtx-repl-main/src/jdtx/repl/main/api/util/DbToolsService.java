package jdtx.repl.main.api.util;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.jdx_db_object.*;

public class DbToolsService extends CompRt {

    public IDbNamesManager getDbNamesManager(Db db) {
        IDbNamesManager inst;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            inst = new DbNamesManager_Oracle();
        } else if (dbType.equalsIgnoreCase("firebird")) {
            inst = new DbNamesManager_Firebird();
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return inst;
    }

    public IDbErrors getDbErrors(Db db) {
        IDbErrors inst;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            inst = new DbErrors_Oracle();
        } else if (dbType.equalsIgnoreCase("firebird")) {
            inst = new DbErrors_Firebird();
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return inst;
    }

    public static IDbObjectManager getDbObjectManager(Db db) {
        IDbObjectManager inst;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            inst = new DbObjectManager_Oracle(db);
        } else if (dbType.equalsIgnoreCase("firebird")) {
            inst = new DbObjectManager_Firebird(db);
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return inst;
    }

}
