package jdtx.repl.main.api.rec_merge;

import java.util.*;

/**
 * Правило, которое описывает группу связанных полей.
 * Мап, где для каждого поля, члена этой группы, храним список полей этой группы (так удобно пользоваться).
 * f1:[f1,f2,f3], f2:[f1,f2,f3], f3:[f1,f2,f3]
 */
public class GroupStrategy extends HashMap<String, Collection<String>> {

    /**
     * @return Возвращает список связанных полей для поля fieldName
     */
    public Collection<String> getForField(String fieldName) {
        if (this.containsKey(fieldName)) {
            // Есть правило - возвращаем список связанных полей для поля fieldName
            return this.get(fieldName);
        } else {
            // Нет правила - возвращаем список, сотоящий из одного поля fieldName
            ArrayList<String> res = new ArrayList<>();
            res.add(fieldName);
            return res;
        }
    }

}
