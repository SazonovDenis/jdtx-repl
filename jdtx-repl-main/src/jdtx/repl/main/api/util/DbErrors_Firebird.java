package jdtx.repl.main.api.util;

import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.struct.*;

public class DbErrors_Firebird extends DbErrorsService implements IDbErrors {

    public boolean errorIs_PrimaryKeyError(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        return errText.contains("violation of PRIMARY or UNIQUE KEY constraint");
    }

    public boolean errorIs_ForeignKeyViolation(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("violation of FOREIGN KEY constraint") && errText.contains("on table")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_TableNotExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if ((errText.contains("table/view") && errText.contains("does not exist") ||
                errText.contains("Table") && errText.contains("does not exist")) ||
                errText.contains("Table unknown")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_GeneratorNotExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("Generator not found") ||
                errText.contains("Generator") && errText.contains("not found")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_TriggerNotExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("Trigger not found") ||
                errText.contains("Trigger") && errText.contains("not found")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_TableAlreadyExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("Table") && errText.contains("already exists")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_GeneratorAlreadyExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("DEFINE GENERATOR failed") && errText.contains("attempt to store duplicate value")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_TriggerAlreadyExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("DEFINE TRIGGER failed") && errText.contains("attempt to store duplicate value")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean errorIs_IndexAlreadyExists(Exception e) {
        String errText = UtJdxErrors.collectExceptionText(e);
        if (errText.contains("attempt to store duplicate value (visible to active transactions) in unique index")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * По тексту ошибки возвращает таблицу, в которой содержится неправильная ссылка
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxTable - таблица, в которой содержится неправильная ссылка, например: Lic
     */
    public IJdxTable get_ForeignKeyViolation_tableInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct) {
        //
        String errText = e.getMessage();
        String[] sa = errText.split("on table");
        //
        String thisTableName = sa[1];
        thisTableName = thisTableName.replace("\"", "").replace(" ", "");
        IJdxTable thisTable = struct.getTable(thisTableName);
        //
        return thisTable;
    }

    /**
     * По тексту ошибки возвращает поле, которое содержит неправильную ссылку
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxForeignKey - ссылочное поле, которое привело к ошибке, например: Lic.Ulz
     */
    public IJdxForeignKey get_ForeignKeyViolation_refInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct) {
        //
        String errText = e.getMessage();
        String[] sa = errText.split("on table");
        //
        String foreignKeyName = sa[0].split("FOREIGN KEY constraint")[1];
        foreignKeyName = foreignKeyName.replace("\"", "").replace(" ", "");
        //
        String thisTableName = sa[1];
        thisTableName = thisTableName.replace("\"", "").replace(" ", "");
        IJdxTable thisTable = struct.getTable(thisTableName);
        //
        for (IJdxForeignKey foreignKey : thisTable.getForeignKeys()) {
            if (foreignKey.getName().compareToIgnoreCase(foreignKeyName) == 0) {
                return foreignKey;
            }
        }
        //
        return null;
    }

}
