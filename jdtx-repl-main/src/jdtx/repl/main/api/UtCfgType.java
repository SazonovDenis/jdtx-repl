package jdtx.repl.main.api;

import jandcode.utils.error.*;

/**
 * Виды конфигураций в БД
 */
public class UtCfgType {

    static final String PUBLICATIONS = "cfg_publications";
    static final String DECODE = "cfg_decode";
    static final String WS = "cfg_ws";

    static void validateCfgCode(String cfgCode) {
        switch (cfgCode) {
            case UtCfgType.PUBLICATIONS:
            case UtCfgType.DECODE:
            case UtCfgType.WS: {
                break;
            }
            default: {
                throw new XError("Unknown cfg type: " + cfgCode);
            }
        }
    }

}
