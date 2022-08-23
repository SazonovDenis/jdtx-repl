package jdtx.repl.main.api.struct.hierarchy;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class JdxDbStructHierarchyIndex {

    private final List<JdxDbStructHierarchyNode> nodes = new ArrayList<>();

    public Set<JdxDbStructHierarchyNode> getRoots() {
        TreeSet<JdxDbStructHierarchyNode> roots = new TreeSet<>();

        for (JdxDbStructHierarchyNode node : nodes) {
            if (node.getChilds().size() != 0 && node.getParents().size() == 0) {
                roots.add(node);
            }
        }

        return roots;
    }

    public Set<JdxDbStructHierarchyNode> getLeafs() {
        TreeSet<JdxDbStructHierarchyNode> roots = new TreeSet<>();

        for (JdxDbStructHierarchyNode node : nodes) {
            if (node.getChilds().size() == 0 && node.getParents().size() != 0) {
                roots.add(node);
            }
        }

        return roots;
    }

    public Set<JdxDbStructHierarchyNode> getAlones() {
        TreeSet<JdxDbStructHierarchyNode> roots = new TreeSet<>();

        for (JdxDbStructHierarchyNode node : nodes) {
            if (node.getChilds().size() == 0 && node.getParents().size() == 0) {
                roots.add(node);
            }
        }

        return roots;
    }

    public JdxDbStructHierarchyNode findNode(String tableName) {
        for (JdxDbStructHierarchyNode node : nodes) {
            if (node.getTable().getName().equalsIgnoreCase(tableName)) {
                return node;
            }
        }
        return null;
    }

    public JdxDbStructHierarchyNode getOrAddNode(IJdxTable table) {
        JdxDbStructHierarchyNode node = findNode(table.getName());
        if (node == null) {
            node = new JdxDbStructHierarchyNode(table);
            nodes.add(node);
        }
        return node;
    }

}
