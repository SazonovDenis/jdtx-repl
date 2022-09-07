package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public class DbGenerators {

    Db db;
    IDbErrors dbErrors;

    static Log log = LogFactory.getLog("jdtx.DbGenerators");

    public DbGenerators(Db db) {
        this.db = db;
        this.dbErrors = DbToolsService.getDbErrors(db);
    }

}
