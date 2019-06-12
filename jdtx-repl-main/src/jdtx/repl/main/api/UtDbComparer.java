package jdtx.repl.main.api;

import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;


// todo: сделать и  проверить сравнение BLOB
public class UtDbComparer {


    public static boolean dbStructIsEqual(IJdxDbStruct struct1, IJdxDbStruct struct2) throws Exception {
        IJdxDbStruct structDiff0 = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn1 = new JdxDbStruct();
        IJdxDbStruct structDiffNewIn2 = new JdxDbStruct();
        return UtDbComparer.dbStructDiff(struct1, struct2, structDiff0, structDiffNewIn1, structDiffNewIn2);
    }

    /**
     * Получает две структуры БД, вычисляет между ними разницу
     *
     * @param structDiffNewIn1 возвращает объекты (таблицы и их поля) в первой БД, которых нет во второй
     * @param structDiffNewIn2 возвращает объекты во второй БД, которых нет впервой
     * @param struct1          структура для сравнения
     * @param struct2          структура для сравнения
     * @param structCommon     возвращает общие объекты в обеих БД
     * @return =true, если структуры БД одинаковые, иначе в structDiff*** возвращается разница
     */
    public static boolean dbStructDiff(IJdxDbStruct struct1, IJdxDbStruct struct2, IJdxDbStruct structCommon, IJdxDbStruct structDiffNewIn1, IJdxDbStruct structDiffNewIn2) throws Exception {
        // сравниваем таблицы из первой базы с таблицами со второй базы
        // таблицы первой базы
        for (IJdxTableStruct t1 : struct1.getTables()) {
            // находим такую же таблицу во второй базе
            IJdxTableStruct t2 = struct2.getTable(t1.getName());
            // если она существует
            if (t2 != null) {
                JdxTableStruct t = null;
                // поля из первой таблицы ищем во второй таблице
                for (IJdxFieldStruct f1 : t1.getFields()) {
                    IJdxFieldStruct f2 = t2.getField(f1.getName());
                    // если поле не существует, создаем новый экземпляр таблицы и добавляем его туда
                    if (f2 == null) {
                        if (t == null) {
                            t = new JdxTableStruct();
                            t.setName(t1.getName());
                        }
                        t.getFields().add(f1);
                    } else {
                        // сравниваем характеристики поля
                        if (!compareField(f1, f2)) {
                            // если поля разные по свойствам, создаем новый экземпляр таблицы и добавляем его туда
                            if (t == null) {
                                t = new JdxTableStruct();
                                t.setName(t1.getName());
                            }
                            t.getFields().add(f1);
                        }
                    }
                }
                if (t != null) {
                    structDiffNewIn1.getTables().add(t);
                } else {
                    structCommon.getTables().add(t1);
                }
            } else {
                structDiffNewIn1.getTables().add(t1);
            }
        }

        // сравниваем таблицы из второй базы с таблицами из первой базы
        // таблицы второй базы
        for (IJdxTableStruct t2 : struct2.getTables()) {
            // находим такую же таблицу в первой базе
            IJdxTableStruct t1 = struct1.getTable(t2.getName());
            // если она существует
            if (t1 != null) {
                JdxTableStruct t = null;
                // поля из второй таблицы ищем в первой таблице
                for (IJdxFieldStruct f2 : t2.getFields()) {
                    IJdxFieldStruct f1 = t1.getField(f2.getName());
                    // если поле не существует, создаем новый экземпляр таблицы и добавляем его туда
                    if (f1 == null) {
                        if (t == null) {
                            t = new JdxTableStruct();
                            t.setName(t2.getName());
                        }
                        t.getFields().add(f2);
                    } else {
                        // сравниваем характеристики поля
                        if (!compareField(f1, f2)) {
                            // если поля разные по свойствам, создаем новый экземпляр таблицы и добавляем его туда
                            if (t == null) {
                                t = new JdxTableStruct();
                                t.setName(t1.getName());
                            }
                            t.getFields().add(f1);
                        }
                    }
                }
                if (t != null) {
                    structDiffNewIn2.getTables().add(t);
                }

            } else {
                structDiffNewIn2.getTables().add(t2);
            }
        }

        // если структуры обеих баз не отличаются возвращаем true
        if (structDiffNewIn1.getTables().size() == 0 && structDiffNewIn2.getTables().size() == 0) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean compareField(IJdxFieldStruct f1, IJdxFieldStruct f2) {
/*
        if (f1 == null) {
            return false;
        }
        if (f2 == null) {
            return false;
        }
        if (f1.getDbDatatype() == null) {
            return false;
        }
        if (f2.getDbDatatype() == null) {
            return false;
        }
*/

        if (f1.getDbDatatype().compareToIgnoreCase(f2.getDbDatatype()) != 0) {
            return false;
        }
        if (f1.getSize() != f2.getSize()) {
            return false;
        }
        return true;
    }


    public static String calcDbStructCrc(IJdxDbStruct struct) throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        return UtString.md5Str(struct_rw.toString(struct));
    }

}
