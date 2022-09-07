package jdtx.repl.main.api.pk_generator;

/**
 * Реализация IAppPkRules для PS.
 * todo: хорошо бы реализовать прочие генераторы (номер билета)
 */
public class AppPkRulesRules_PS extends AppPkRulesService implements IAppPkRules {

    @Override
    public String getGeneratorName(String tableName) {
        return "g_" + tableName;
    }

}
