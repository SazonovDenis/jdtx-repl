package jdtx.repl.main.api.jdx_db_object;

import jandcode.app.*;
import jandcode.utils.error.*;
import jandcode.utils.rt.*;

public class SqlScriptExecutorService extends CompRt {

    public ISqlScriptExecutor createByName(String name) {
        Rt rt = getRt().findChild("item/" + name);

        //
        if (rt == null) {
            throw new XError("Не удалось найти скрипт: " + name);
        }

        //
        return (ISqlScriptExecutor) getApp().getObjectFactory().create(rt);
    }


}
