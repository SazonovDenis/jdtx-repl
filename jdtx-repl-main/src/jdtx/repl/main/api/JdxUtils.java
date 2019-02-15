package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;

import java.util.*;

public class JdxUtils {

    public static final String prefix = "Z_";

    public static final String audit_table_prefix = prefix;

    public static final String sys_table_prefix = prefix + "Z_";

    public static final String audit_gen_prefix = prefix + "G_";

    public static final String sys_gen_prefix = sys_table_prefix + "G_";

    public static final String trig_pref = prefix + "T_";

    /**
     * ��������� lst � ������ foreign key.
     * ��������� ����� ��������� ��� ����������� ������� ������ ��� ins.
     *
     * @param lst �������� ������
     * @return ��������������� ������
     */
    public static ArrayList<IJdxTableStruct> sortTables(ArrayList<IJdxTableStruct> lst) {
        // ��������������� ������ ������
        ArrayList<IJdxTableStruct> sortLst = new ArrayList<IJdxTableStruct>();

        // ������ ������, ������� ��� ����� � sortLst
        ArrayList<IJdxTableStruct> usedLst = new ArrayList<IJdxTableStruct>();

        // ������ ������, ������� ��� �� ����� � sortLst (���� ��� �������)
        ArrayList<IJdxTableStruct> restLst = new ArrayList<IJdxTableStruct>();
        restLst.addAll(lst);

        int i = 0;
        // � ������ ������� � sortLst �������� �������, �� ����������� �� ������ �������
        while (i < restLst.size()) {
            // ���� ��� ������� ������
            if (restLst.get(i).getForeignKeys().size() == 0) {
                sortLst.add(restLst.get(i));
                usedLst.add(restLst.get(i));
                restLst.remove(i);
            } else {
                i++;
            }
        }

        // ��� ���� ����������� ������ ���� ��������� ������� � ��������� �� � sortLst �� ��� ���, ���� � sortLst �� ����� ��� �������
        while (restLst.size() != 0) {
            // ������ ������, ����������� �� ������ ��������
            ArrayList<IJdxTableStruct> curLst = new ArrayList<IJdxTableStruct>();

            // ���� �������, ����������� �� ��� ��������� � usedtLst �������
            i = 0;
            while (i < restLst.size()) {
                IJdxTableStruct table = restLst.get(i);

                // ���������� ��� ������� ����� ������� table � �������� �����, ���������-�� ��� �� ������� �� ��� ��������������� (usedLst)
                boolean willAdd = true;
                for (IJdxForeignKey fk : table.getForeignKeys()) {
                    // ���� ������� ��������� �� �� ��������� � usedLst ������� (� ��� ������ �� �� ���� ����), ��� �� ����� ���������
                    if (!fk.getTable().getName().equalsIgnoreCase(table.getName()) && usedLst.indexOf(fk.getTable()) == -1) {
                        willAdd = false;
                        break;
                    }
                }

                //
                if (willAdd) {
                    // ������� ��������� ������ �� ����-���� �� ��� ��������������� (usedLst)
                    sortLst.add(table);
                    restLst.remove(i);
                    curLst.add(table);
                } else {
                    i++;
                }
            }

            // � ������ �������������� ������ ���������� ������ ������, ����������� �� ������ ��������
            usedLst.addAll(curLst);
        }

        // ��������������� ������ ������
        return sortLst;
    }


}
