package jdtx.repl.main.api.pk_generator;

/**
 * Особенности работы pk по правилам приложения.
 */
public interface IAppPkRules {

    /**
     * По имени таблицы возвращает имя генератора для нее.
     * Выражает соглашение по именам генераторв в приложении.
     */
    String getGeneratorName(String tableName) throws Exception;

}
