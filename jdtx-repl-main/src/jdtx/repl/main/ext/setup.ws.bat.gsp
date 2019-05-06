@echo off 

call jc repl-create -ws:${args.no} -guid:${args.guid_repl}-${args.guid}

call jc repl-snapshot 

call jc repl-info
