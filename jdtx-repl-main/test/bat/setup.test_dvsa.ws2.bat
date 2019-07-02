@echo off 

rem служба
call jc repl-service-remove -csc


rem рабочая станция "ws2"
call jc repl-create -ws:2 -guid:413DF2C54CC9D2A2.test_dvsa-02E4A13756A63160


rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

