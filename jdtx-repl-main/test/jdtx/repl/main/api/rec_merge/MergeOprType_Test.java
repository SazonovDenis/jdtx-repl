package jdtx.repl.main.api.rec_merge;

import junit.framework.*;
import org.junit.Test;

public class MergeOprType_Test extends TestCase {

    @Test
    public void test() throws Exception {
        System.out.println("MergeOprType.UPD: " + MergeOprType.UPD);
        System.out.println("MergeOprType.DEL: " + MergeOprType.DEL);
        //
        System.out.println("MergeOprType.UPD: " + MergeOprType.UPD.getValue());
        System.out.println("MergeOprType.DEL: " + MergeOprType.DEL.getValue());
        //
        System.out.println("MergeOprType.UPD: " + MergeOprType.valueOf("UPD"));
        System.out.println("MergeOprType.DEL: " + MergeOprType.valueOf("DEL"));
    }

}