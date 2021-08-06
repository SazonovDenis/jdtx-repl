package jdtx.repl.main.task;

import jdtx.repl.main.ut.*;

import java.util.*;

public class JdxErrorCollector {

    //
    List<Map> errors = new ArrayList<>();

    //
    public void collectError(String info, Exception e) {
        Map infoMap = new HashMap();
        infoMap.put("operation", info);
        infoMap.put("error", Ut.getExceptionMessage(e));
        errors.add(infoMap);
    }

    public List<Map> getErrors() {
        return errors;
    }

}
