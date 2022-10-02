package jdtx.repl.main.api.struct.hierarchy;

import jandcode.utils.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;
import org.junit.*;

import java.io.*;
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
        db_one.connect();
    }

    @Test
    public void getNodesInfo() throws Exception {
        System.out.println("db: " + UtJdx.getDbInfoStr(db));

        // ---
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
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
        System.out.println("db: " + UtJdx.getDbInfoStr(db));

        // ---
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
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

    @Test
    public void printNodesHierarchyJson() throws Exception {
        System.out.println("db: " + UtJdx.getDbInfoStr(db_one));

        // ---
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db_one);
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

        JSONArray object;
/*
        // ---
        Set<JdxDbStructHierarchyNode> set = new HashSet<>();
        set.add(index.findNode("LIC"));
        //
        System.out.println("Hierarchy main -> dict: LIC");
        object = JdxDbStructHierarchyUtils.getNodesTreeJson(set);
        System.out.println(object);
        System.out.println();
*/

        // ---
/*
        System.out.println("Roots: TreeJson");
        object = JdxDbStructHierarchyUtils.getNodesTreeJson(index.getRoots());
        System.out.println(object);
        System.out.println();
        UtFile.saveString(object.toString(), new File("temp/roots.json"));
*/

        // ---
        System.out.println("All: TreeJson");
        object = JdxDbStructHierarchyUtils.getNodesTreeJson(index.getAll());
        System.out.println(object);
        System.out.println();
        UtFile.saveString(object.toString(), new File("temp/all.json"));

/*
        // ---
        System.out.println("Roots: PlainJson");
        JSONArray object = JdxDbStructHierarchyUtils.getNodesPlainJson(index.getRoots());
        System.out.println(object);
*/
    }


}
