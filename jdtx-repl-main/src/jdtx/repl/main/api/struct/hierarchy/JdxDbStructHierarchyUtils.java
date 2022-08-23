package jdtx.repl.main.api.struct.hierarchy;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class JdxDbStructHierarchyUtils {

    public static JdxDbStructHierarchyIndex makeStructHierarchy(IJdxDbStruct struct) {
        JdxDbStructHierarchyIndex index = new JdxDbStructHierarchyIndex();

        //
        for (IJdxTable table : struct.getTables()) {
            JdxDbStructHierarchyNode node = index.getOrAddNode(table);

            for (IJdxForeignKey fk : table.getForeignKeys()) {
                JdxDbStructHierarchyNode nodeDesc = index.getOrAddNode(fk.getTable());
                node.getChilds().add(nodeDesc);
                nodeDesc.getParents().add(node);
            }
        }

        //
        return index;
    }

    public static List<Map> getNodesInfo(Set<JdxDbStructHierarchyNode> nodes) {
        List res = new ArrayList();

        for (JdxDbStructHierarchyNode node : nodes) {
            res.add(getNodeInfo(node));
        }

        return res;
    }

    private static Map getNodeInfo(JdxDbStructHierarchyNode node) {
        Map info = new HashMap();

        //
        info.put("name", node.getTable().getName());
        info.put("childsCount", node.getChilds().size());
        info.put("childsCountFull", getChildsCount(node));

        //
        List childs = new ArrayList();
        for (JdxDbStructHierarchyNode nodeChild : node.getChilds()) {
            if (node.getTable() == nodeChild.getTable()) {
                continue;
            }

            childs.add(getNodeInfo(nodeChild));
        }
        info.put("childs", childs);

        //
        return info;
    }

    public static void printNodes(Set<JdxDbStructHierarchyNode> nodes) {
        for (JdxDbStructHierarchyNode node : nodes) {
            printNode(node, "");
        }
    }

    public static void printNodesHierarchyChilds(Set<JdxDbStructHierarchyNode> nodes) {
        for (JdxDbStructHierarchyNode node : nodes) {
            printNodesHierarchyChilds(node, "");
        }
    }

    public static void printNodesHierarchyParents(Set<JdxDbStructHierarchyNode> nodes) {
        for (JdxDbStructHierarchyNode node : nodes) {
            printNodesHierarchyParents(node, "");
        }
    }


    //-------------
    //-------------
    //-------------


    private static void printNode(JdxDbStructHierarchyNode node, String prefix) {
        System.out.println(prefix + node.getTable().getName() + getChildsStr(node));
    }

    private static String getChildsStr(JdxDbStructHierarchyNode node) {
        if (node.getChilds().size() == 0) {
            return " [0]";
        } else {
            return " [" + node.getChilds().size() + " - " + getChildsCount(node) + "]";
        }
    }

    private static int getChildsCount(JdxDbStructHierarchyNode node) {
        int count = node.getChilds().size();

        for (JdxDbStructHierarchyNode child : node.getChilds()) {
            if (node.getTable() == child.getTable()) {
                count = count - 1;
                continue;
            }

            //
            count = count + getChildsCount(child);
        }

        return count;
    }

    private static void printNodesHierarchyChilds(JdxDbStructHierarchyNode root, String prefix) {
        printNode(root, prefix);

        prefix = prefix + "  ";
        for (JdxDbStructHierarchyNode node : root.getChilds()) {
            if (root.getTable() == node.getTable()) {
                System.out.println(prefix + "self ref: " + root.getTable().getName());
                continue;
            }

            //
            printNodesHierarchyChilds(node, prefix);
        }
    }

    private static void printNodesHierarchyParents(JdxDbStructHierarchyNode root, String prefix) {
        printNode(root, prefix);

        prefix = prefix + "  ";
        for (JdxDbStructHierarchyNode node : root.getParents()) {
            if (root.getTable() == node.getTable()) {
                System.out.println(prefix + "self ref: " + root.getTable().getName());
                continue;
            }

            //
            printNodesHierarchyParents(node, prefix);
        }
    }


}
