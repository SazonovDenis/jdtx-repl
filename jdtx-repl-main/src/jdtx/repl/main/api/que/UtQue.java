package jdtx.repl.main.api.que;

import jandcode.utils.error.*;

public class UtQue {


    // Имена очередей
    public static final String QUE_NONE = null;

    // Имена очередей: на рабочей станции
    public static final String QUE_IN = "in";
    public static final String QUE_IN001 = "in001";
    public static final String QUE_OUT = "out";

    // Имена очередей: на сервере
    public static final String QUE_COMMON = "common";
    public static final String QUE_OUT000 = "out000";
    public static final String QUE_OUT001 = "out001";

    public static String getTableSuffix(String queName) {
        switch (queName) {
            case QUE_IN: {
                return "in";
            }
            case QUE_IN001: {
                return "in001";
            }
            case QUE_OUT: {
                return "out";
            }
            case QUE_COMMON: {
                return "common";
            }
            case QUE_OUT000: {
                return "out000";
            }
            case QUE_OUT001: {
                return "out001";
            }
            default: {
                throw new XError("invalid queName: " + queName);
            }
        }
    }


    // Где хранится состояние очереди (используется для префиксов таблиц состояния): для каждой станции свой (используется для очередей станций на сервере)
    public static final boolean STATE_AT_SRV = true;

    // Где хранится состояние очереди (используется для префиксов таблиц состояния): один единственный (используется для личных очередей рабочей станции)
    public static final boolean STATE_AT_WS = false;


}
