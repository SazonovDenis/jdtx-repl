package jdtx.repl.main.api.util;

import jandcode.app.*;
import jandcode.dbm.db.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.pk_generator.*;

public class DbToolsService extends CompRt {

    public static IDbNamesManager getDbNamesManager(Db db) {
        IDbNamesManager instance;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            instance = new DbNamesManager_Oracle();
        } else if (dbType.equalsIgnoreCase("firebird")) {
            instance = new DbNamesManager_Firebird();
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return instance;
    }

    public static IDbErrors getDbErrors(Db db) {
        IDbErrors instance;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            instance = new DbErrors_Oracle();
        } else if (dbType.equalsIgnoreCase("firebird")) {
            instance = new DbErrors_Firebird();
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return instance;
    }

    public static IDbObjectManager getDbObjectManager(Db db) {
        IDbObjectManager instance;
        instance = new UtDbObjectManager(db);
        return instance;
    }

    public static IDbGenerators getDbGenerators(Db db) {
        IDbGenerators instance;
        String dbType = UtJdx.getDbType(db);
        if (dbType.equalsIgnoreCase("oracle")) {
            instance = new DbGenerators_Oracle(db);
        } else if (dbType.equalsIgnoreCase("firebird")) {
            instance = new DbGenerators_Firebird(db);
        } else {
            throw new XError("Неизвестный тип базы: " + dbType);
        }
        return instance;
    }

}
