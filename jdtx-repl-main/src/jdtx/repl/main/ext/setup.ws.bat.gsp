@echo off 

rem служба
call jc repl-service-remove -csc


rem рабочая станция "${args.ws_name}"
call jc repl-create -ws:${args.ws_no} -guid:${args.repl_guid}-${args.ws_guid}


rem служба
call jc repl-service-install


rem для сведения
call jc repl-info

