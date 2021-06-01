rem @echo off

rem set cfg_path=%~dp0
set cfg_path=C:\Users\Public\Documents\Jadatex.Sync\web\WEB-INF\cfg\

rem cd ..

@echo -------------------------
@echo %cfg_path%
@echo -------------------------

rem Напрямую задаем структуру публикаций
call jc repl_set_cfg -cfg:cfg_publications -file:%cfg_path%publication_lic_194_ws.json