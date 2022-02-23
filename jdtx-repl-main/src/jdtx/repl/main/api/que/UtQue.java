package jdtx.repl.main.api.que;

import jandcode.utils.error.*;
import jdtx.repl.main.api.util.*;

public class UtQue {


    // todo заменить константы "Имена очередей" на ENUM
    // Имена очередей: на рабочей станции
    public static final String QUE_IN = "in";
    public static final String QUE_IN001 = "in001";
    public static final String QUE_OUT = "out";

    // Имена очередей: на сервере
    public static final String SRV_QUE_IN = "srv_in";
    public static final String SRV_QUE_COMMON = "common";
    public static final String SRV_QUE_OUT000 = "out000";
    public static final String SRV_QUE_OUT001 = "out001";

    public static String getQueName(String queName) {
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
            case SRV_QUE_IN: {
                return "in";
            }
            case SRV_QUE_COMMON: {
                return "common";
            }
            case SRV_QUE_OUT000: {
                return "out000";
            }
            case SRV_QUE_OUT001: {
                return "out001";
            }
            default: {
                throw new XError("invalid queName: " + queName);
            }
        }
    }

    public static String getQueTableName(String queName) {
        switch (queName) {
            case QUE_IN: {
                return UtJdx.SYS_TABLE_PREFIX + "que_in";
            }
            case QUE_IN001: {
                return UtJdx.SYS_TABLE_PREFIX + "que_in001";
            }
            case QUE_OUT: {
                return UtJdx.SYS_TABLE_PREFIX + "que_out";
            }
            case SRV_QUE_IN: {
                return UtJdx.SYS_TABLE_PREFIX + "srv_que_in";
            }
            case SRV_QUE_COMMON: {
                return UtJdx.SYS_TABLE_PREFIX + "srv_que_common";
            }
            case SRV_QUE_OUT000: {
                return UtJdx.SYS_TABLE_PREFIX + "srv_que_out000";
            }
            case SRV_QUE_OUT001: {
                return UtJdx.SYS_TABLE_PREFIX + "srv_que_out001";
            }
            default: {
                throw new XError("invalid queName: " + queName);
            }
        }
    }

    public static String getQueTableGeneratorName(String queName) {
        switch (queName) {
            case QUE_IN: {
                return UtJdx.SYS_GEN_PREFIX + "que_in";
            }
            case QUE_IN001: {
                return UtJdx.SYS_GEN_PREFIX + "que_in001";
            }
            case QUE_OUT: {
                return UtJdx.SYS_GEN_PREFIX + "que_out";
            }
            case SRV_QUE_IN: {
                return UtJdx.SYS_GEN_PREFIX + "srv_que_in";
            }
            case SRV_QUE_COMMON: {
                return UtJdx.SYS_GEN_PREFIX + "srv_que_common";
            }
            case SRV_QUE_OUT000: {
                return UtJdx.SYS_GEN_PREFIX + "srv_que_out000";
            }
            case SRV_QUE_OUT001: {
                return UtJdx.SYS_GEN_PREFIX + "srv_que_out001";
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
