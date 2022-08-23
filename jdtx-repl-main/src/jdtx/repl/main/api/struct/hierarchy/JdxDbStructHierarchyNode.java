package jdtx.repl.main.api.struct.hierarchy;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class JdxDbStructHierarchyNode implements IJdxDbStructHierarchyNode, Comparable {

    IJdxTable table;
    Set<JdxDbStructHierarchyNode> parents;
    Set<JdxDbStructHierarchyNode> childs;

    public JdxDbStructHierarchyNode(IJdxTable table) {
        this.table = table;
        this.parents = new TreeSet<>();
        this.childs = new TreeSet<>();
    }

    @Override
    public IJdxTable getTable() {
        return table;
    }

    @Override
    public Set<JdxDbStructHierarchyNode> getParents() {
        return parents;
    }

    @Override
    public Set<JdxDbStructHierarchyNode> getChilds() {
        return childs;
    }

    @Override
    public int compareTo(Object o) {
        JdxDbStructHierarchyNode other = (JdxDbStructHierarchyNode) o;
        return this.getTable().getName().compareToIgnoreCase(other.getTable().getName());
    }
}
