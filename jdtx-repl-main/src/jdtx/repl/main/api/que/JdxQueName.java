package jdtx.repl.main.api.que;

import jandcode.utils.error.*;

public class JdxQueName {

    public static final String NONE = null;

    // На рабочей станции
    public static final String IN = "in";
    public static final String IN001 = "in001";
    public static final String OUT = "out";

    // На сервере
    public static final String COMMON = "common";
    public static final String OUT001 = "out001";

    public static String getTableSuffix(String queName) {
        switch (queName) {
            case IN: {
                return "in";
            }
            case IN001: {
                return "in001";
            }
            case OUT: {
                return "out";
            }
            case COMMON: {
                return "common";
            }
            case OUT001: {
                return "out001";
            }
            default: {
                throw new XError("invalid queName: " + queName);
            }
        }
    }

}
