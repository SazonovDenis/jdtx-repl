package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import java.util.*;

public class RecDuplicate {
    /**
     * Поля и их значения - ключ для поикса дублей
     * Например:
     * Name -> МВД РК,
     * Cod -> 012000
     */
    Map params;

    /**
     * Список записей, содержащих дубликаты
     */
    DataStore records;
}
