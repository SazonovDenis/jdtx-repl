package jdtx.repl.main.api.struct.hierarchy;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class JdxDbStructHierarchy_Test extends DbPrepareEtalon_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../../ext/";
        super.setUp();
        db.connect();
    }

    @Test
    public void getNodesInfo() throws Exception {
        // ---
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        System.out.println("db: " + db.getDbSource().getDatabase());
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        System.out.println("---");
        System.out.println();


        // ---
        JdxDbStructHierarchyIndex index = JdxDbStructHierarchyUtils.makeStructHierarchy(struct);


        // ---
        System.out.println(JdxDbStructHierarchyUtils.getNodesInfo(index.getRoots()));
    }

    @Test
    public void printNodesHierarchy() throws Exception {
        // ---
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        System.out.println("db: " + db.getDbSource().getDatabase());
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        System.out.println("---");
        System.out.println();


        // ---
        JdxDbStructHierarchyIndex index = JdxDbStructHierarchyUtils.makeStructHierarchy(struct);


        // ---
        System.out.println("Alones:");
        System.out.println();
        JdxDbStructHierarchyUtils.printNodes(index.getAlones());
        System.out.println("---");
        System.out.println();

        // ---
        System.out.println("Roots:");
        System.out.println();
        JdxDbStructHierarchyUtils.printNodes(index.getRoots());
        System.out.println("---");
        System.out.println();

        // ---
        System.out.println("Leafs:");
        System.out.println();
        JdxDbStructHierarchyUtils.printNodes(index.getLeafs());
        System.out.println("---");
        System.out.println();

        // ---
        System.out.println("Hierarchy main -> dict:");
        System.out.println();
        JdxDbStructHierarchyUtils.printNodesHierarchyChilds(index.getRoots());
        System.out.println("---");
        System.out.println();

        // ---
        //System.out.println("Hierarchy dict -> main:");
        //System.out.println();
        //JdxDbStructHierarchyUtils.printNodesHierarchyParents(index.getLeafs());
        //System.out.println("---");
        //System.out.println();

        // ---
        Set<JdxDbStructHierarchyNode> set = new HashSet<>();
        set.add(index.findNode("LIC"));
        //
        System.out.println("Hierarchy main -> dict: LIC");
        System.out.println();
        JdxDbStructHierarchyUtils.printNodesHierarchyChilds(set);
        System.out.println("---");
        System.out.println();
    }


}
