package jdtx.repl.main.api.replica;

import jandcode.utils.*;
import jandcode.utils.error.*;

/**
 * Виды операций в реплике (IDE)
 */
public enum JdxOprType {

    OPR_INS(1),
    OPR_UPD(2),
    OPR_DEL(3);

    int value;

    JdxOprType(int value) {
        this.value = value;
    }

    public static JdxOprType valueOfInt(int oprTypeInt) {
        for (JdxOprType oprType : JdxOprType.values()) {
            if (oprType.getValue() == oprTypeInt) {
                return oprType;
            }
        }
        throw new XError("Not valid oprType: " + oprTypeInt);
    }

    public static JdxOprType valueOfStr(String oprTypeStr) {
        if (UtCnv.toInt(oprTypeStr, 0) != 0) {
            return valueOfInt(UtCnv.toInt(oprTypeStr));
        } else {
            return valueOf(oprTypeStr);
        }
    }

    public int getValue() {
        return value;
    }

}
