package jdtx.repl.main.api.replica;

import junit.framework.*;
import org.junit.Test;

public class JdxOprType_Test extends TestCase {

    @Test
    public void test_1() {
        String oprType_1_str = String.valueOf(JdxOprType.INS);
        System.out.println("oprType_1: " + oprType_1_str);

        JdxOprType oprType_2 = JdxOprType.valueOfInt(1);
        String oprType_2_str = String.valueOf(oprType_2);
        System.out.println("oprType_2: " + oprType_2_str);

        String oprType_3_str = "OPR_INS";
        JdxOprType oprType_3 = JdxOprType.valueOfStr(oprType_3_str);
        System.out.println("oprTypeStr: " + oprType_3_str + ", oprType: " + oprType_3);

        String oprType_4_str = "1";
        JdxOprType oprType_4 = JdxOprType.valueOfStr(oprType_4_str);
        System.out.println("oprTypeStr: " + oprType_4_str + ", oprType: " + oprType_4);
    }

}
