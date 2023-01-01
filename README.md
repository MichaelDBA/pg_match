# pg_match
Schema Summary Comparison Tool

(c) 2022-2023 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

## Example Usage:
pg_match.py -t simplescan --Shost localhost --Sport 5414 --Suser postgres --Sdb clone_testing --Sschema sample --Thost localhost --Tport 5414 --Tuser postgres --Tdb clone_testing --Tschema sample_clone1

## Screen Shot

![image](https://user-images.githubusercontent.com/12436545/187948655-a1717907-646a-4464-8756-561f5f23e830.png)

## Parameters
<br/>

`-t --scantype`          SimpleScan or DetailedScan
<br/>
`-H --Shost`            Source host
<br/>
`-P --Sport`            Source port
<br/>
`-U --Suser`            Source DB user
<br/>
`-D --Sdb`              Source databae
<br/>
`-S --Sschema`          Source schema
<br/>
`-h --Thost`            Target host
<br/>
`-p --Tport`            Target port
<br/>
`-u --Tuser`            Target DB user
<br/>
`-d --Tdb`              Target databae
<br/>
`-s --Tschema`          Target schema
<br/>
`-r --ignore_rowcounts` Bypass row count check
<br/>
`-i --ignore_indexes`   Bypass index check
<br/>
`-f --ignore_funcs`     Bypass function/procedure check
<br/>
`-l --log`              log diffs to specified output file
<br/>
`-v --verbose`          verbose output (useful for debugging)
<br/>
<br/>

## Requirements
1. python 2.7 or above
2. python packages: psycopg2
3. Works on Linux and Windows.
4. PostgreSQL versions 9.6 and up
<br/>

## Assumptions
None at this time
