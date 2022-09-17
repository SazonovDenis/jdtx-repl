package jdtx.repl.main.api.pk_generator;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;

public abstract class DbGeneratorsService extends DbMember implements IDbGenerators {

    static Log log = LogFactory.getLog("jdtx.DbGenerators");

    public IDbErrors getDbErrors() {
        return getDb().service(DbErrorsService.class);
    }

}
