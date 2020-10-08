package jdtx.repl.main.api.rec_merge;

import jandcode.dbm.data.*;

import java.util.*;

public class MergeResultTable {

    /**
     * Удаленные записи в таблице (как были до удаления)
     */
    DataStore recordsDeleted;

    /**
     * Обновленные записи в зависимых таблицах:
     * что пришлось сделать с каждой из зависимых таблиц,
     * чтобы сделать merge в основной таблице
     */
    Map<String, MergeResultRefTable> mergeResultRefTable = new HashMap<>();

}
