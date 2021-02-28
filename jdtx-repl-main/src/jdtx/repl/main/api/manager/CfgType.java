package jdtx.repl.main.api.manager;

import jandcode.utils.error.*;

/**
 * Виды конфигураций в БД
 */
public class CfgType {

    public static final String PUBLICATIONS = "cfg_publications";
    public static final String DECODE = "cfg_decode";
    public static final String WS = "cfg_ws";

    static void validateCfgCode(String cfgCode) {
        switch (cfgCode) {
            case CfgType.PUBLICATIONS:
            case CfgType.DECODE:
            case CfgType.WS: {
                break;
            }
            default: {
                throw new XError("Unknown cfg type: " + cfgCode);
            }
        }
    }

}
