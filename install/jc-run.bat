@echo on

SET DRIVE_LETTER=%~d0
cd /d %DRIVE_LETTER%

SET CURR_DIR=%~dp0
cd /d %CURR_DIR%

jc run label:jdtx.repl.main.task

