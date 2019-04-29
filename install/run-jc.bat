@echo on

SET DRIVE_LETTER=%~d0
cd /d %DRIVE_LETTER%

SET CURR_DIR=%~dp0
cd /d %CURR_DIR%

rem jc run > output.msg 2> output.err

jc run label:jdtx.repl.main.task

