
# acctdv2

This is simply used to generate the appropriate values
and structure to create an account for EA.  It does no
error or format checking and does not check for duplicate
Account names.  If you use the -I option it

```
NAME:
   acctdv2 - Tool used to create Account entries

USAGE:
   acctdv2 [global options]

VERSION:
   0.0.1

COMMANDS:
GLOBAL OPTIONS:
   -N 			Account Name required field
   -I 			Account UUID used to rewrite account data
   --help, -h		show help
   --version, -v	print the version
```

## CREATE AN ACCOUNT:
-----------------
1. Run acctdv2 to generate the oort-cli statements:

```
acctdv2 -N dummy2
write /token d5cabec1-009d-4560-8632-9b1bd842a9af {"token":"d5cabec1-009d-4560-8632-9b1bd842a9af","acctid":"b118469d-07b3-4c76-935c-ad9bae53e6e0"}

write /acct b118469d-07b3-4c76-935c-ad9bae53e6e0 {"id":"b118469d-07b3-4c76-935c-ad9bae53e6e0","name":"dummy2","token":"d5cabec1-009d-4560-8632-9b1bd842a9af","status":"active","createdate":1462206025,"deletedate":0}
```

2.  Open the oort-cli client and execute these statements in group mode.   




## LIST OF ACCOUNTS:
----------------
1. Open the oort-cli client and Run
read-group /acct



## UPDATE ACCOUNT WITH A NEW TOKEN:
-----------------------------------
1. Run acctdv2 with the -I option to generate a new token but leave the rest

```
acctdv2 -N dummy2 -I b118469d-07b3-4c76-935c-ad9bae53e6e0
write /token e9174c77-417f-4c08-9c96-989f1ba5309b {"token":"e9174c77-417f-4c08-9c96-989f1ba5309b","acctid":"11e4eb19-190e-4e4d-9726-b81ceed098a2"}

write /acct 11e4eb19-190e-4e4d-9726-b81ceed098a2 {"id":"11e4eb19-190e-4e4d-9726-b81ceed098a2","name":"dummy2","token":"e9174c77-417f-4c08-9c96-989f1ba5309b","status":"active","createdate":1462218231,"deletedate":0}
```

2. Open the oort-cli client and execute these statements



## SHOW INFORMATION ABOUT AN ACCOUNT
------------------------------------
1. Open oort-cli client and run
read /acct  <ACCOUNT ID>



## DELETE AN ACCOUNT
--------------------
1. Open oort-cli client and run
delete /acct <ACCOUNT ID>    
