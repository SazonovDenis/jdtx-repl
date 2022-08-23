package jdtx.repl.main.api.struct.hierarchy;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public interface IJdxDbStructHierarchyNode {

    IJdxTable getTable();

    Set<JdxDbStructHierarchyNode> getParents();

    Set<JdxDbStructHierarchyNode> getChilds();

}
