package jdtx.repl.main.api.util;

import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.struct.*;

public interface IUtDbErrors {

    boolean errorIs_PrimaryKeyError(Exception e);

    boolean errorIs_ForeignKeyViolation(Exception e);

    boolean errorIs_TableNotExists(Exception e);

    boolean errorIs_GeneratorNotExists(Exception e);

    boolean errorIs_TriggerNotExists(Exception e);

    boolean errorIs_TableAlreadyExists(Exception e);

    boolean errorIs_GeneratorAlreadyExists(Exception e);

    boolean errorIs_TriggerAlreadyExists(Exception e);

    boolean errorIs_IndexAlreadyExists(Exception e);

    /**
     * По тексту ошибки возвращает таблицу, в которой содержится неправильная ссылка
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxTable - таблица, в которой содержится неправильная ссылка, например: Lic
     */
    IJdxTable get_ForeignKeyViolation_tableInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct);

    /**
     * По тексту ошибки возвращает поле, которое содержит неправильную ссылку
     *
     * @param e Exception, например: violation of FOREIGN KEY constraint "FK_LIC_ULZ" on table "LIC"
     * @return IJdxForeignKey - ссылочное поле, которое привело к ошибке, например: Lic.Ulz
     */
    IJdxForeignKey get_ForeignKeyViolation_refInfo(JdxForeignKeyViolationException e, IJdxDbStruct struct);

}
