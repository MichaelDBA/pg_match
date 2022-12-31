#!/usr/bin/env python2
#!/usr/bin/env python3
#!/usr/bin/env python
##########################################################################################
# File Name: pg_match.py
# Description:
# This file is a python script file which compares 2 schemas for data consistency.
# It is functional for Postgresql only at this time. It is totally rewritten using psycopg2 module instead of pgdb module.
#
# External Interfaces: Postgresql databases (normally master and slave) # # Assumptions:
# 01.  Functional for Postgresql databases at this time.
# 02.  Assumes the secret database credentials file (.dbcompare) is populated correctly.
#
# usage:
# type = simplescan | detailedscan
# pg_match.py -t simplescan -H localhost --Sport 5414 --Suser postgres --Sdb clone_testing --Sschema sample --Thost localhost --Tport 5414 --Tuser postgres --Tdb clone_testing --Tschema sample_clone1
# pg_match.py -t simplescan -H eddp-staging-popstore3.cluster-cddj9wcflvor.us-gov-west-1.rds.amazonaws.com  --Sport 5432 --Suser root --Sdb popstore3 --Sschema lookup_module --Thost eddp-staging-popstore3.cluster-cddj9wcflvor.us-gov-west-1.rds.amazonaws.com --Tport 5432 --Tuser root --Tdb popstore3 --Tschema lookup_module_beta
# Assumptions:
# 1. Only supports PG versions 10 and up.
# 2. Both versions must be v10 if either one of them is v10.
# 3. Python 2.7+, 3.x+ are supported python versions.
#
# Copyright (C) 2013 - 2023, SQLExec LLC.  All rights reserved.
#
# Modifications History:
# Date          Programmer        Description of Change
# ==========    ==========        =====================
# 2013-11-29    Michael Vitale    Original Coding
# 2014-06-26    Michael Vitale    Ported to Postgresql: big problem-->information_schema.tables does not have a table_rows column!
# 2016-07-02    Michael Vitale    Major rewrite: Changed name from pg_comparedatabases.py to pg_match.py
#                                 procedural to object oriented, i.e., class instantiation
#                                 changed logic for what gets done for rowcounts: compare pg_class.reltuples, pg_stat_user_tables.n_live_tup, then actual count if necessary
# 2022-05-29    Michael Vitale    version 2.0: Major rewrite. Made compatible with python 2.7 and 3.x
# 2022-06-12    Michael Vitale    version 2.1: Make backward compatible with v10 (pg_proc.prokind)
# 2022-06-14    Michael Vitale    version 2.1: Added view check. Fixed table/view check by adding join to information_schema.tables. Detailed table check.
# 2022-06-17    Michael Vitale    version 2.1: Change detailedscan to mean real rowcounts.
# 2022-06-19    Michael Vitale    version 2.1: Add logic for comparing keys and indexes.
# 2022-09-01    Michael Vitale    version 2.2: Replace pg_auth with pg_roles to be compatible with PG cloud services. Fixed formatting to allow for longer names.
# 2023-01-02    Michael Vitale    version 2.3: Fix column comparison for column differences, not attribute ones which are handled correctly.
##########################################################################################
import string, curses, sys, os, subprocess, time, datetime, types, warnings, random, getpass
from optparse  import OptionParser
from decimal import *
import psycopg2

DESCRIPTION="This python utility program compares schemas for a specific database."
VERSION    = 2.3
PROGNAME   = "pg_match"
ADATE      = "January 2, 2023"
PROGDATE   = "2023--01-02"

#Globals
FAIL = 1
SUCCESS = 0

# return code constants
RC_OK    = 0
RC_ERR   = 1
RC_WARN  = 2
RC_FATAL = 3
RC_DIFF  = 4

INFO  ="INFO  "
SQL   = "SQL  "
DEBUG ="DEBUG "
WARN  ="WARN  "
ERR   ="ERROR "
FATAL ="FATAL "
DIFF  ="DIFF  "

class maint:
    def __init__(self):
        self.connS             = False
        self.connT             = False
        self.curS              = False
        self.curT              = False
        self.connstrS          = ''
        self.connstrT          = ''
        self.scantype          = ''
        self.flog              = False
        self.logging           = False
        self.verbose           = False
        self.logsql            = ''
        self.logfile           = ''

        # connection parms
        self.Shost             = ''
        self.Sport             = -1
        self.Suser             = ''
        self.Sdb               = ''
        self.Sschema           = ''
        self.Thost             = ''
        self.Tport             = -1
        self.Tuser             = ''
        self.Tdb               = ''
        self.Tschema           = ''        
        self.ddldiffs          = 0;
        self.rowcntdiffs       = 0;
        self.is_prokind        = True;
        self.pg_version_numS   = 0;
        self.pg_version_numT   = 0;


    #######################
    # logging function    #
    #######################
    def logit(self, severity, msg):

        if not self.verbose and severity == "DEBUG ":
            return

        if not self.flog and self.logging:
            now = datetime.datetime.now().strftime("%Y_%m_%d")
            path = os.getcwd()
            path += "/"
            self.logfile = "%spg_match_%s.log" % (path,now)
            self.flog = open(self.logfile, 'a')

        now = datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S ")
        if self.logging:
            self.flog.write(severity + now + msg + "\n")
        if self.verbose:
            print (now + ' ' + msg)
        else:    
            print (msg)
        return self.flog

    ##########################
    # close stuff gracefully #
    ##########################
    def CloseStuff(self):

        # rollback any unintentional changes
        if self.connS is not None:
            self.connS.rollback()
        if self.connT is not None:            
            self.connT.rollback()

        if self.curS is not None:
            self.curS.close()
            self.connS.close()
    
        if self.connT is not None:    
            self.curT.close()        
            self.connT.close()

        if self.flog:
            self.flog.close()

        return


    #######################
    # Connection function #
    #######################
    def ConnectAll(self):
        # get connection and cursor handles for source and target databases
    
        self.connstrS = "dbname=%s port=%d user=%s host=%s application_name=%s" % (self.Sdb, self.Sport, self.Suser, self.Shost, PROGNAME)
        try:
            self.connS = psycopg2.connect(self.connstrS)
        except Exception as error:
            msg="Source Connection Error %s *** %s" % (type(error), error)
            if 'fe_sendauth: no password supplied' in msg:
                # prompt them for password and try again
                apass = getpass.getpass(prompt='Enter Source DB Password: ')
                self.connstrS = "dbname=%s port=%d user=%s host=%s application_name=%s password=%s" % (self.Sdb, self.Sport, self.Suser, self.Shost, PROGNAME, apass)
                try:
                    self.connS = psycopg2.connect(self.connstrS)
                except Exception as error:
                    msg="Source Connection Error %s *** %s" % (type(error), error)                    
                    self.logit(ERR, msg)
                    return RC_ERR
                self.curS = self.connS.cursor()                    
            else:                    
                # some other connection error
                self.logit(ERR, msg)
                return RC_ERR
        self.curS = self.connS.cursor()

     
        self.connstrT = "dbname=%s port=%d user=%s host=%s application_name=%s" % (self.Tdb, self.Tport, self.Tuser, self.Thost, PROGNAME)
        try:
            self.connT = psycopg2.connect(self.connstrT)
        except Exception as error:
            msg="Target Connection Error %s *** %s" % (type(error), error)
            if 'fe_sendauth: no password supplied' in msg:
                # prompt them for password and try again
                apass = getpass.getpass(prompt='Enter Target DB Password: ')
                self.connstrT = "dbname=%s port=%d user=%s host=%s application_name=%s password=%s" % (self.Tdb, self.Tport, self.Tuser, self.Thost, PROGNAME, apass)
                try:
                    self.connT = psycopg2.connect(self.connstrT)
                except Exception as error:
                    msg="Target Connection Error %s *** %s" % (type(error), error)                    
                    self.logit(ERR, msg)
                    return RC_ERR
                self.curT = self.connT.cursor()                    
            else:                    
                self.logit(ERR, msg)
                return RC_ERR
        self.curT = self.connT.cursor()

        # For compatibility with PG V10, check if pg_proc.prokind exists.  If not determine function another way.
        sql = "SELECT count(*) FROM pg_attribute WHERE  attrelid = 'pg_proc'::regclass AND attname = 'prokind'"
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Schema Version Check Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curS.fetchone()
        if len(arow) == 0:
            msg="Source schema Version Check Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        if arow[0] == 0:
            self.is_prokind = False;
        else:
            self.is_prokind = True;

        # Target schema must also be v10 if source is v10.
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Schema Version Check Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curT.fetchone()
        if len(arow) == 0:
            msg="Target schema Version Check Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        if (not self.is_prokind and arow[0] > 0) or (self.is_prokind and arow[0] == 0):
            # version mismatch
            msg="For backward compatibility, Source and Target schemas must both be v10 if either one of them is v10."
            self.logit(ERR, msg)
            return RC_ERR            

        # get source and target PG version numbers for logic later...	
        sql = "SELECT setting FROM pg_settings WHERE name = 'server_version_num'"
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source PG Version Check Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curS.fetchone()
        if len(arow) == 0:
            msg="Source PG Version Check Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        self.pg_version_numS = int(arow[0])
        if self.pg_version_numS < 100000:
          msg = "Source PG Version Number: %d   PG Versions older than v10 are not supported." % self.pg_version_numS
          self.logit(ERR, msg)
          return RC_ERR    

        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target PG Version Check Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curT.fetchone()
        if len(arow) == 0:
            msg="Target PG Version Check Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        self.pg_version_numT = int(arow[0])
        if self.pg_version_numT < 100000:
          msg = "Target PG Version Number: %d   PG Versions older than v10 are not supported." % self.pg_version_numT
          self.logit(ERR, msg)
          return RC_ERR    

        # Validate schemas exist
        sql = "SELECT count(*) FROM pg_namespace n WHERE n.nspname = '%s'" % self.Sschema
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Schema Validation Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curS.fetchone()
        if len(arow) == 0:
            msg="Source schema Validation Count Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        if arow[0] == 0:
            msg="Source schema (%s) not found." % self.Sschema
            self.logit(ERR, msg)
            return RC_ERR            
            
        sql = "SELECT count(*) FROM pg_namespace n WHERE n.nspname = '%s'" % self.Tschema
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Schema Validation Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        arow = self.curT.fetchone()
        if len(arow) == 0:
            msg="Target schema Validation Count Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        if arow[0] == 0:
            msg="Target schema (%s) not found." % self.Tschema
            self.logit(ERR, msg)
            return RC_ERR                
        

        return RC_OK

    ###############################
    # Phase 1: object count diffs #
    ###############################
    def CompareObjects(self):
        # do all objects first except comments since comments are quite complex
        aschema = self.Sschema
        sql = "SELECT rt.tbls_regular as tbls_regular, ut.unlogged_tables as tbls_unlogged, pt.partitions as tbls_child, pn.parents as tbls_parents, " \
              "rt.tbls_regular + ut.unlogged_tables + pt.partitions + pn.parents as tbls_total, ft.ftables as ftables, se.sequences as sequences, ide.identities as identities, ix.indexes as indexes, " \
              "vi.views as views, pv.pviews as pub_views, mv.mats as mat_views, fn.functions as functions, ty.types as types, tf.trigfuncs, tr.triggers as triggers, " \
              "co.collations as collations, dom.domains as domains, ru.rules as rules, po.policies as policies FROM " \
              "(SELECT count(*) as tbls_regular FROM pg_class c, pg_tables t, pg_namespace n where t.schemaname = '%s' and t.tablename = c.relname and c.relkind = 'r' and " \
              "    n.oid = c.relnamespace and n.nspname = t.schemaname and c.relpersistence = 'p' and c.relispartition is false) rt, " \
              "(SELECT count(distinct (t.schemaname, t.tablename)) as unlogged_tables from pg_tables t, pg_class c where t.schemaname = '%s' and t.tablename = c.relname and c.relkind = 'r' and c.relpersistence = 'u' ) ut, " \
              "(SELECT count(*) as ftables FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'f' AND n.nspname = '%s') ft, " \
              "(SELECT count(*) as sequences FROM pg_class c, pg_namespace n where n.oid = c.relnamespace and c.relkind = 'S' and n.nspname = '%s') se, " \
              "(SELECT count(*) as identities FROM pg_sequences where schemaname = '%s' AND NOT EXISTS (select 1 from information_schema.sequences where sequence_schema = '%s' and sequence_name = sequencename)) ide, " \
              "(SELECT count(*) as indexes from pg_class c, pg_namespace n, pg_indexes i where n.nspname = '%s' and n.oid = c.relnamespace and c.relkind != 'p' and n.nspname = i.schemaname and c.relname = i.tablename) ix, " \
              "(SELECT count(*) as views from pg_views where schemaname = '%s') vi, (select count(*) as pviews from pg_views where schemaname = 'public') pv, " \
              "(SELECT count(distinct i.inhparent) as parents from pg_inherits i, pg_class c, pg_namespace n  where c.relkind in ('p','r') and i.inhparent = c.oid and c.relnamespace = n.oid and n.nspname = '%s') pn, " \
              "(SELECT count(*) as partitions FROM pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid) JOIN pg_class as p ON (inhparent=p.oid) JOIN pg_namespace pn ON pn.oid = p.relnamespace " \
              "    JOIN pg_namespace cn ON cn.oid = c.relnamespace WHERE pn.nspname = '%s' and c.relkind = 'r') pt, " \
              "(SELECT count(*) as functions FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid) WHERE ns.nspname = '%s') fn, " \
              "(SELECT count(*) as types FROM pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR " \
              "    (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname = '%s') ty, " \
              "(SELECT count(*) as trigfuncs FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang " \
              "    WHERE pg_catalog.pg_get_function_result(p.oid) = 'trigger' and n.nspname = '%s') tf, " \
              "(SELECT count(distinct (trigger_schema, trigger_name, event_object_table, action_statement, action_orientation, action_timing)) as triggers  FROM information_schema.triggers WHERE trigger_schema = '%s') tr, " \
              "(SELECT count(distinct(n.nspname, c.relname)) as mats from pg_class c, pg_namespace n where c.relnamespace = n.oid and c.relkind = 'm') mv, " \
              "(SELECT count(*) as collations FROM pg_collation c JOIN pg_namespace n ON (c.collnamespace = n.oid) JOIN pg_roles a ON (c.collowner = a.oid) WHERE n.nspname = '%s') co, " \
              "(SELECT count(*) as domains FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE t.typtype = 'd' AND n.nspname OPERATOR(pg_catalog.~) '^(%s)$' COLLATE pg_catalog.default) dom, " \
              "(SELECT count(*) as rules from pg_rules where schemaname = '%s') ru, " \
              "(SELECT count(*) as policies from pg_policies where schemaname = '%s') po" \
              % (aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, 
                 aschema, aschema, aschema, aschema, aschema)
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Object Count Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        arow = self.curS.fetchone()
        if len(arow) == 0:
            msg="Source Object Count Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        #print (arow)
        tbls_regular  = arow[0]
        tbls_unlogged = arow[1]
        tbls_child    = arow[2]
        tbls_parents  = arow[3]
        tbls_total    = arow[4]
        tbls_foreign  = arow[5]
        sequences     = arow[6]
        identities    = arow[7]
        indexes       = arow[8]
        views         = arow[9]
        pub_views     = arow[10]
        mat_views     = arow[11]
        functions     = arow[12]
        types         = arow[13]
        trigfuncs     = arow[14]
        triggers      = arow[15]
        collations    = arow[16]
        domains       = arow[17]
        rules         = arow[18]
        policies      = arow[19]
        
        aschema = self.Tschema
        sql = "SELECT rt.tbls_regular as tbls_regular, ut.unlogged_tables as tbls_unlogged, pt.partitions as tbls_child, pn.parents as tbls_parents, " \
              "rt.tbls_regular + ut.unlogged_tables + pt.partitions + pn.parents as tbls_total, ft.ftables as ftables, se.sequences as sequences, ide.identities as identities, ix.indexes as indexes, " \
              "vi.views as views, pv.pviews as pub_views, mv.mats as mat_views, fn.functions as functions, ty.types as types, tf.trigfuncs, tr.triggers as triggers, " \
              "co.collations as collations, dom.domains as domains, ru.rules as rules, po.policies as policies FROM " \
              "(SELECT count(*) as tbls_regular FROM pg_class c, pg_tables t, pg_namespace n where t.schemaname = '%s' and t.tablename = c.relname and c.relkind = 'r' and " \
              "    n.oid = c.relnamespace and n.nspname = t.schemaname and c.relpersistence = 'p' and c.relispartition is false) rt, " \
              "(SELECT count(distinct (t.schemaname, t.tablename)) as unlogged_tables from pg_tables t, pg_class c where t.schemaname = '%s' and t.tablename = c.relname and c.relkind = 'r' and c.relpersistence = 'u' ) ut, " \
              "(SELECT count(*) as ftables FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'f' AND n.nspname = '%s') ft, " \
              "(SELECT count(*) as sequences FROM pg_class c, pg_namespace n where n.oid = c.relnamespace and c.relkind = 'S' and n.nspname = '%s') se, " \
              "(SELECT count(*) as identities FROM pg_sequences where schemaname = '%s' AND NOT EXISTS (select 1 from information_schema.sequences where sequence_schema = '%s' and sequence_name = sequencename)) ide, " \
              "(SELECT count(*) as indexes from pg_class c, pg_namespace n, pg_indexes i where n.nspname = '%s' and n.oid = c.relnamespace and c.relkind != 'p' and n.nspname = i.schemaname and c.relname = i.tablename) ix, " \
              "(SELECT count(*) as views from pg_views where schemaname = '%s') vi, (select count(*) as pviews from pg_views where schemaname = 'public') pv, " \
              "(SELECT count(distinct i.inhparent) as parents from pg_inherits i, pg_class c, pg_namespace n  where c.relkind in ('p','r') and i.inhparent = c.oid and c.relnamespace = n.oid and n.nspname = '%s') pn, " \
              "(SELECT count(*) as partitions FROM pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid) JOIN pg_class as p ON (inhparent=p.oid) JOIN pg_namespace pn ON pn.oid = p.relnamespace " \
              "    JOIN pg_namespace cn ON cn.oid = c.relnamespace WHERE pn.nspname = '%s' and c.relkind = 'r') pt, " \
              "(SELECT count(*) as functions FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid) WHERE ns.nspname = '%s') fn, " \
              "(SELECT count(*) as types FROM pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR " \
              "    (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname = '%s') ty, " \
              "(SELECT count(*) as trigfuncs FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang " \
              "    WHERE pg_catalog.pg_get_function_result(p.oid) = 'trigger' and n.nspname = '%s') tf, " \
              "(SELECT count(distinct (trigger_schema, trigger_name, event_object_table, action_statement, action_orientation, action_timing)) as triggers  FROM information_schema.triggers WHERE trigger_schema = '%s') tr, " \
              "(SELECT count(distinct(n.nspname, c.relname)) as mats from pg_class c, pg_namespace n where c.relnamespace = n.oid and c.relkind = 'm') mv, " \
              "(SELECT count(*) as collations FROM pg_collation c JOIN pg_namespace n ON (c.collnamespace = n.oid) JOIN pg_roles a ON (c.collowner = a.oid) WHERE n.nspname = '%s') co, " \
              "(SELECT count(*) as domains FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE t.typtype = 'd' AND n.nspname OPERATOR(pg_catalog.~) '^(%s)$' COLLATE pg_catalog.default) dom, " \
              "(SELECT count(*) as rules from pg_rules where schemaname = '%s') ru, " \
              "(SELECT count(*) as policies from pg_policies where schemaname = '%s') po" \
              % (aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, 
                 aschema, aschema, aschema, aschema, aschema)        
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Object Count Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        arow = self.curT.fetchone()
        if len(arow) == 0:
            msg="Target Object Count Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
        #print (arow)

        # do the compare        
        msg = ''
        typediff = 'Object Count Diff:'
        if tbls_regular != arow[0]:
            msg = '%20s      Regular table mismatch (%.3d<>%.3d)' % (typediff, tbls_regular, arow[0])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)
        if tbls_unlogged != arow[1]:
            msg = '%20s     Unlogged table mismatch (%.3d<>%.3d)' % (typediff, tbls_unlogged, arow[1])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)
        if tbls_child != arow[2]:
            msg = '%20s        Child table mismatch (%.3d<>%.3d)' % (typediff, tbls_child, arow[2])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)        
        if tbls_parents != arow[3]:
            msg = '%20s       Parent table mismatch (%.3d<>%.3d)' % (typediff, tbls_parents, arow[3])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if tbls_total != arow[4]:
            msg = '%20s        Total table mismatch (%.3d<>%.3d)' % (typediff, tbls_total, arow[4])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)        
        if tbls_foreign != arow[5]:
            msg = '%20s      Foreign table mismatch (%.3d<>%.3d)' % (typediff, tbls_foreign, arow[5])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)            
        if sequences  != arow[6]:
            msg = '%20s          Sequences mismatch (%.3d<>%.3d)' % (typediff, sequences, arow[6])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if identities != arow[7]:
            msg = '%20s         Identities mismatch (%.3d<>%.3d)' % (typediff, identities, arow[7])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if indexes != arow[8]:
            msg = '%20s            Indexes mismatch (%.3d<>%.3d)' % (typediff, indexes, arow[8])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if views != arow[9]:
            msg = '%20s              Views mismatch (%.3d<>%.3d)' % (typediff, views, arow[9])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if pub_views != arow[10]:
            msg = '%20s       Public Views mismatch (%.3d<>%.3d)' % (typediff, pub_views, arow[10])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if mat_views != arow[11]:
            msg = '%20s Materialized Views mismatch (%.3d<>%.3d)' % (typediff, mat_views, arow[11])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if functions != arow[12]:
            msg = '%20s          Functions mismatch (%.3d<>%.3d)' % (typediff, functions, arow[12])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if types != arow[13]:
            msg = '%20s              Types mismatch (%.3d<>%.3d)' % (typediff, types, arow[13])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if trigfuncs != arow[14]:
            msg = '%20s  Trigger Functions mismatch (%.3d<>%.3d)' % (typediff, trigfuncs, arow[14])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if triggers != arow[15]:
            msg = '%20s           Triggers mismatch (%.3d<>%.3d)' % (typediff, triggers, arow[15])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if collations != arow[16]:
            msg = '%20s         Collations mismatch (%.3d<>%.3d)' % (typediff, collations, arow[16])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if domains != arow[17]:
            msg = '%20s            Domains mismatch (%.3d<>%.3d)' % (typediff, domains, arow[17])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if rules != arow[18]:
            msg = '%20s              Rules mismatch (%.3d<>%.3d)' % (typediff, rules, arow[18])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                
        if policies != arow[19]:
            msg = '%20s           Policies mismatch (%.3d<>%.3d)' % (typediff, policies, arow[19])
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                

        # Now do the comments compare
        aschema = self.Sschema
        
        if self.is_prokind:
            proc_sql = "SELECT CASE WHEN p.prokind = 'f' THEN 'FUNCTION' WHEN p.prokind = 'p' THEN 'PROCEDURE' WHEN p.prokind = 'a' THEN 'AGGREGATE FUNCTION' WHEN p.prokind = 'w' THEN 'WINDOW FUNCTION' END as OBJECT, " \
	               "p.proname as relname, d.description as comments from pg_catalog.pg_namespace n  " \
	               "JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname = '%s' " % (aschema)
        else:
            proc_sql = "SELECT CASE WHEN proisagg THEN 'AGGREGATE ' ELSE 'FUNCTION ' END as OBJECT, " \
	               "p.proname as relname, d.description as comments from pg_catalog.pg_namespace n  " \
	               "JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname = '%s' " % (aschema)           
        
        sql = \
            "WITH details as (SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' " \
            "WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname as relname, d.description as comments " \
	    "FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) LEFT JOIN pg_description d ON (c.oid = d.objoid) LEFT JOIN pg_attribute a ON (c.oid = a.attrelid AND a.attnum > 0 and a.attnum = d.objsubid) " \
	    "WHERE d.objsubid = 0 AND d.description IS NOT NULL AND n.nspname = '%s' " \
	    "UNION  " \
	    "SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' " \
	    "WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, " \
	    "c.relname::text as relname, d.description as comments from pg_namespace n, pg_description d, pg_class c where d.objoid = c.oid and c.relnamespace = n.oid and d.objsubid = 0 AND n.nspname = '%s' " \
	    "UNION  " \
	    "SELECT 'COLUMN' as OBJECT, s.column_name as relname, d.description as comments FROM pg_description d, pg_class c, pg_namespace n, information_schema.columns s  " \
	    "WHERE d.objsubid > 0 and d.objoid = c.oid and c.relnamespace = n.oid and n.nspname = '%s' and s.table_schema = n.nspname and s.table_name = c.relname and s.ordinal_position = d.objsubid " \
	    "UNION " \
	    "SELECT 'DOMAIN' as OBJECT, t.typname as relname, d.description as comments from pg_description d, pg_type t, pg_namespace n  where d.description = 'my domain comments on addr' and " \
	    "d.objoid = t.oid and t.typtype = 'd' and t.typnamespace = n.oid AND d.objsubid = 0 AND n.nspname = '%s' " \
	    "UNION " \
	    "SELECT 'SCHEMA' as OBJECT, n.nspname as relname, d.description as comments FROM pg_description d, pg_namespace n WHERE d.objsubid = 0 AND d.classoid::regclass = 'pg_namespace'::regclass AND " \
	    "d.objoid = n.oid and n.nspname = '%s'  " \
	    "UNION " \
	    "SELECT 'TYPE' AS OBJECT, t.typname as relname, pg_catalog.obj_description(t.oid, 'pg_type') as comments FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR " \
	    "(SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el " \
	    "WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname = '%s' AND pg_catalog.obj_description(t.oid, 'pg_type') IS NOT NULL AND t.typtype = 'c' " \
	    "UNION  " \
	    "SELECT 'COLATION' as OBJECT, collname as relname,  pg_catalog.obj_description(c.oid, 'pg_collation') as comments FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n WHERE n.oid = c.collnamespace AND  " \
	    "c.collencoding IN (-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND n.nspname = '%s' AND pg_catalog.obj_description(c.oid, 'pg_collation') IS NOT NULL  " \
	    "UNION " \
	    "%s" \
	    "UNION " \
	    "SELECT 'POLICY' as OBJECT, p1.policyname as relname, d.description as comments from pg_policies p1, pg_policy p2, pg_class c, pg_namespace n, pg_description d WHERE d.objsubid = 0 AND " \
	    "p1.schemaname = n.nspname and p1.tablename = c.relname AND " \
	    "n.oid = c.relnamespace and c.relkind in ('r','p') and p1.policyname = p2.polname and d.objoid = p2.oid and p1.schemaname = '%s' ORDER BY 1) " \
	    "SELECT object, count(*) from details group by 1 order by 1" \
	    % (aschema, aschema, aschema, aschema, aschema, aschema, aschema, proc_sql, aschema)
	    
	    ## % (aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema, aschema)
	    ## "SELECT CASE WHEN p.prokind = 'f' THEN 'FUNCTION' WHEN p.prokind = 'p' THEN 'PROCEDURE' WHEN p.prokind = 'a' THEN 'AGGREGATE FUNCTION' WHEN p.prokind = 'w' THEN 'WINDOW FUNCTION' END as OBJECT, " \
	    ## "p.proname as relname, d.description as comments from pg_catalog.pg_namespace n  " \
	    ## "JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname = '%s' " \
        try:              
            # print ("DEBUG: %s" % sql)
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Comments Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        Srows = self.curS.fetchall()

        aschema = self.Tschema
        if self.is_prokind:
            proc_sql = "SELECT CASE WHEN p.prokind = 'f' THEN 'FUNCTION' WHEN p.prokind = 'p' THEN 'PROCEDURE' WHEN p.prokind = 'a' THEN 'AGGREGATE FUNCTION' WHEN p.prokind = 'w' THEN 'WINDOW FUNCTION' END as OBJECT, " \
	               "p.proname as relname, d.description as comments from pg_catalog.pg_namespace n  " \
	               "JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname = '%s' " % (aschema)
        else:
            proc_sql = "SELECT CASE WHEN proisagg THEN 'AGGREGATE ' ELSE 'FUNCTION ' END as OBJECT, " \
	               "p.proname as relname, d.description as comments from pg_catalog.pg_namespace n  " \
	               "JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname = '%s' " % (aschema)           
        
        sql = \
            "WITH details as (SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' " \
            "WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname as relname, d.description as comments " \
	    "FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) LEFT JOIN pg_description d ON (c.oid = d.objoid) LEFT JOIN pg_attribute a ON (c.oid = a.attrelid AND a.attnum > 0 and a.attnum = d.objsubid) " \
	    "WHERE d.objsubid = 0 AND d.description IS NOT NULL AND n.nspname = '%s' " \
	    "UNION  " \
	    "SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' " \
	    "WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, " \
	    "c.relname::text as relname, d.description as comments from pg_namespace n, pg_description d, pg_class c where d.objoid = c.oid and c.relnamespace = n.oid and d.objsubid = 0 AND n.nspname = '%s' " \
	    "UNION  " \
	    "SELECT 'COLUMN' as OBJECT, s.column_name as relname, d.description as comments FROM pg_description d, pg_class c, pg_namespace n, information_schema.columns s  " \
	    "WHERE d.objsubid > 0 and d.objoid = c.oid and c.relnamespace = n.oid and n.nspname = '%s' and s.table_schema = n.nspname and s.table_name = c.relname and s.ordinal_position = d.objsubid " \
	    "UNION " \
	    "SELECT 'DOMAIN' as OBJECT, t.typname as relname, d.description as comments from pg_description d, pg_type t, pg_namespace n  where d.description = 'my domain comments on addr' and " \
	    "d.objoid = t.oid and t.typtype = 'd' and t.typnamespace = n.oid AND d.objsubid = 0 AND n.nspname = '%s' " \
	    "UNION " \
	    "SELECT 'SCHEMA' as OBJECT, n.nspname as relname, d.description as comments FROM pg_description d, pg_namespace n WHERE d.objsubid = 0 AND d.classoid::regclass = 'pg_namespace'::regclass AND " \
	    "d.objoid = n.oid and n.nspname = '%s'  " \
	    "UNION " \
	    "SELECT 'TYPE' AS OBJECT, t.typname as relname, pg_catalog.obj_description(t.oid, 'pg_type') as comments FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR " \
	    "(SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el " \
	    "WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname = '%s' AND pg_catalog.obj_description(t.oid, 'pg_type') IS NOT NULL AND t.typtype = 'c' " \
	    "UNION  " \
	    "SELECT 'COLATION' as OBJECT, collname as relname,  pg_catalog.obj_description(c.oid, 'pg_collation') as comments FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n WHERE n.oid = c.collnamespace AND  " \
	    "c.collencoding IN (-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND n.nspname = '%s' AND pg_catalog.obj_description(c.oid, 'pg_collation') IS NOT NULL  " \
	    "UNION " \
	    "%s" \
	    "UNION " \
	    "SELECT 'POLICY' as OBJECT, p1.policyname as relname, d.description as comments from pg_policies p1, pg_policy p2, pg_class c, pg_namespace n, pg_description d WHERE d.objsubid = 0 AND " \
	    "p1.schemaname = n.nspname and p1.tablename = c.relname AND " \
	    "n.oid = c.relnamespace and c.relkind in ('r','p') and p1.policyname = p2.polname and d.objoid = p2.oid and p1.schemaname = '%s' ORDER BY 1) " \
	    "SELECT object, count(*) from details group by 1 order by 1" \
	    % (aschema, aschema, aschema, aschema, aschema, aschema, aschema, proc_sql, aschema)
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Comments Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
        Trows = self.curT.fetchall()

        if len(Srows) == 0 and len(Trows) == 0:
            msg="No Comments in either schema."
            self.logit(INFO, msg)

        cntS = 0
        cntT = 0
        typediff = 'Comments Diff:'
        
        for Srow in Srows:
            cntS = cntS + 1
            sObject = Srow[0]
            sCount  = Srow[1]
            for Trow in Trows:
                cntT = cntT + 1
                tObject = Trow[0]
                tCount  = Trow[1]
                if sObject == tObject:
                    if sCount != tCount:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, "%20s %s  source (%d)  target (%d)" % (typediff, sObject, sCount, tCount))

        # loop again for both source and target capturing objects types are are not in the other
        for Srow in Srows:
            sObject = Srow[0]
            sCount  = Srow[1]
            found = False
            for Trow in Trows:
                tObject = Trow[0]
                if tObject == sObject:
                    found = True
                    break
            if not found:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, "%20s %-19s  source comments (%04d) not found in target schema (%s)." % (typediff, sObject, sCount, self.Tschema))

        for Trow in Trows:
            tObject = Trow[0]
            tCount  = Trow[1]
            found = False
            for Srow in Srows:
                sObject = Srow[0]
                if tObject == sObject:
                    found = True
                    break
            if not found:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, "%20s %-19s  target comments (%04d) not found in source schema (%s)." % (typediff, tObject, tCount, self.Sschema))


        print ('')
        
        return RC_OK    

    #############################
    # Phase 2: Table/View Diffs #
    #############################
    def CompareTablesViews(self):
        aschema = self.Sschema
        sql = "SELECT tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM pg_tables WHERE schemaname = '%s' ORDER BY 1" % aschema;
        
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Table Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="Source Table Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    

        aschema = self.Tschema
        sql = "SELECT tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM pg_tables WHERE schemaname = '%s' ORDER BY 1" % aschema;
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Table Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Trows = self.curT.fetchall()
        if len(Trows) == 0:
            msg="Target Table Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    

        cnt = 0
        typediff = 'Tables Diff:'
        
        for Sarow in Srows:
            cnt = cnt + 1
            sTablename   = Sarow[0]
            sTableowner  = Sarow[1]
            sTablespace  = Sarow[2]
            sHasindexes  = Sarow[3]
            sHasrules    = Sarow[4]
            sHastriggers = Sarow[5]
            sRowsecurity = Sarow[6]
            bFound = False
            for Tarow in Trows:
                tTablename = Tarow[0]
                if sTablename == tTablename:
                    bFound = True
                    tTableowner  = Tarow[1]
                    tTablespace  = Tarow[2]
                    tHasindexes  = Tarow[3]
                    tHasrules    = Tarow[4]
                    tHastriggers = Tarow[5]
                    tRowsecurity = Tarow[6]
                    if sTablespace != tTablespace:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s %20s Source TableSpace (%s) <> Target Tablespace (%s)' % (typediff, sTablename, sTablespace, tTablespace))
                    if sHasindexes != tHasindexes:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s %20s Source HasIndexes (%s) <> Target HasIndexes (%s)' % (typediff, sTablename, sHasindexes, tHasindexes))
                    if sHasrules != tHasrules:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s %20s Source HasRules (%s) <> Target HasRules (%s)' % (typediff, sTablename, sHasrules, tHasrules))
                    if sHastriggers != tHastriggers:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s %20s Source HasTriggers (%s) <> Target HasTriggers (%s)' % (typediff, sTablename, sHastriggers, tHastriggers))
                    if sRowsecurity != tRowsecurity:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s %20s Source RowSecurity (%s) <> Target RowSecurity (%s)' % (typediff, sTablename, sRowsecurity, tRowsecurity))

            if not bFound:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, '%20s Source table (%35s) not found in Target' % (typediff, sTablename))

        # Just check if table is missing from source when compared from target
        for Tarow in Trows:
            tTablename   = Tarow[0]
            bFound = False
            for Sarow in Srows:
                 sTablename = Sarow[0]
                 if sTablename == tTablename:
                    bFound = True
                    continue
            if not bFound:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, '%20s Target table (%35s) not found in Source' % (typediff, tTablename))
        

        #### VIEWS CHECK ####
        '''
        SELECT table_name, view_definition, check_option, is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into 
        FROM information_schema.views WHERE table_schema = 'sample' ORDER BY 1;
        '''              
        aschema = self.Sschema
        sql = "SELECT table_name, view_definition, check_option, is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into " \
              "FROM information_schema.views WHERE table_schema = '%s' ORDER BY 1" % aschema;
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source View Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="%20s No views found" % 'Source View Diff:'
            self.logit(INFO, msg)

        aschema = self.Tschema
        sql = "SELECT table_name, view_definition, check_option, is_updatable, is_insertable_into, is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into " \
              "FROM information_schema.views WHERE table_schema = '%s' ORDER BY 1" % aschema;
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target View Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Trows = self.curT.fetchall()
        if len(Trows) == 0:
            msg="%20s No views found" % 'Target View Diff:'             
            self.logit(INFO, msg)

        typediff = 'Views Diff:'
        for Sarow in Srows:
            sViewName                 = Sarow[0]
            sViewDef                  = Sarow[1]
            sCheckOption              = Sarow[2]
            sIsUpdatable              = Sarow[3]
            sIsInsertable_into        = Sarow[4]
            sIsTriggerUpdatable       = Sarow[5]
            sIsTriggerDeletable       = Sarow[6]
            sIsTriggerInsertable_into = Sarow[7]
            bFound = False
            for Tarow in Trows:
                tViewName = Tarow[0]    
                if sViewName == tViewName:
                    bFound = True
                    tViewDef                  = Tarow[1]
                    tCheckOption              = Tarow[2]
                    tIsUpdatable              = Tarow[3]
                    tIsInsertable_into        = Tarow[4]
                    tIsTriggerUpdatable       = Tarow[5]
                    tIsTriggerDeletable       = Tarow[6]
                    tIsTriggerInsertable_into = Tarow[7]
                    
                    # change target schema to source schema before definition comparison
                    tViewDef2 = tViewDef.replace(self.Tschema, self.Sschema)
                    
                    if sViewDef != tViewDef2:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) def <> Target' % (typediff, sViewName))
                    if sCheckOption != tCheckOption:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) CheckOption <> Target' % (typediff, sViewName))
                    if sIsUpdatable != tIsUpdatable:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) IsUpdatable <> Target' % (typediff, sViewName))
                    if sIsInsertable_into != tIsInsertable_into:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) IsInsertable_into <> Target' % (typediff, sViewName))
                    if sIsTriggerUpdatable != tIsTriggerUpdatable:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) IsTriggerUpdatable <> Target' % (typediff, sViewName))
                    if sIsTriggerDeletable != tIsTriggerDeletable:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) IsTriggerDeletable <> Target' % (typediff, sViewName))
                    if sIsTriggerInsertable_into != tIsTriggerInsertable_into:
                        self.ddldiffs = self.ddldiffs + 1
                        self.logit (DIFF, '%20s Source  view (%s) IsTriggerInsertable_into <> Target' % (typediff, sViewName))                        
            if not bFound:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, '%20s Source  view (%s) not found in Target' % (typediff, sViewName))

        
        for Tarow in Trows:
            tViewName                 = Tarow[0]
            bFound = False
            for Sarow in Srows:
                sViewName = Sarow[0]    
                if sViewName == tViewName:
                    bFound = True
                    break
            if not bFound:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, '%20s Target  view (%s) not found in Source' % (typediff, tViewName))



        print ('')
            
        return RC_OK


    ########################
    # Phase 3: Column Diffs #
    ########################
    def CompareColumns(self):
        aschema = self.Sschema
        '''
        SELECT t.table_name, c.ordinal_position, c.column_name, COALESCE(c.column_default, ''), is_nullable, c.data_type, COALESCE(c.character_maximum_length, -1), 
	COALESCE(c.numeric_precision_radix,-1), COALESCE(c.numeric_scale,-1), c.is_identity, c.is_generated 
        FROM information_schema.tables t, information_schema.columns c WHERE t.table_schema = 'sample' AND t.table_type = 'BASE TABLE' AND t.table_catalog = c.table_catalog AND 
        t.table_schema = c.table_schema AND t.table_name = c.table_name order by 1,2;
        '''
        sql = "SELECT t.table_name, c.ordinal_position, c.column_name, COALESCE(c.column_default, ''), is_nullable, c.data_type, COALESCE(c.character_maximum_length, -1), " \
              "COALESCE(c.numeric_precision_radix,-1), COALESCE(c.numeric_scale,-1), c.is_identity, c.is_generated " \
              "FROM information_schema.tables t, information_schema.columns c WHERE t.table_schema = '%s' AND t.table_type = 'BASE TABLE' AND " \
              "t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema AND t.table_name = c.table_name order by 1,2" % aschema;
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Column Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="Source Column Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    

        aschema = self.Tschema
        sql = "SELECT table_name, ordinal_position, column_name, COALESCE(column_default, ''), is_nullable, data_type, COALESCE(character_maximum_length, -1), " \
              "COALESCE(numeric_precision_radix,-1), COALESCE(numeric_scale,-1), is_identity, is_generated " \
              "FROM information_schema.columns WHERE table_schema = '%s' order by 1,2" % aschema;
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Column Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Trows = self.curT.fetchall()
        TargetRows = len(Trows)
        if TargetRows == 0:
            msg="Target Column Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    

        # loop only looking for column differences
        typediff = 'Columns Diff'
        cnt1 = 0
        lastTableName = ''
        for sRow in Srows:
            sTableName   = sRow[0]
            if sTableName == lastTableName:
                continue
            lastTableName = sTableName    
            
            sql1 = "SELECT table_name, string_agg(column_name, ',' ORDER BY column_name) columns FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' GROUP BY 1" % (self.Sschema, sTableName);
            try:              
                self.curS.execute(sql1)
            except Exception as error:
                msg="Source Schema Columns Error %s *** %s" % (type(error), error)
                self.logit(ERR, msg)
                return RC_ERR
            arow = self.curS.fetchone()
            if len(arow) == 0:
                msg="Source schema Columns Error: No rows returned."
                self.logit(ERR, msg)
                return RC_ERR    
            sColumns = arow[1]
            sql2 = "SELECT table_name, string_agg(column_name, ',' ORDER BY column_name) columns FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' GROUP BY 1" % (self.Tschema, sTableName);
            try:              
                self.curT.execute(sql2)
            except Exception as error:
                msg="Target Schema Columns Error %s *** %s" % (type(error), error)
                self.logit(ERR, msg)
                return RC_ERR
            arow = self.curT.fetchone()
            if arow is None:
                msg="          Skipping missing target table, %s." % sTableName
                self.logit(DEBUG, msg)
                continue
            tColumns = arow[1]
            if sColumns != tColumns:
                self.ddldiffs = self.ddldiffs + 1
                self.logit (DIFF, '%20s: Table (%35s) Columns Mismatch' % (typediff, sTableName))              

        # loop again only looking for column attribute differences
        typediff = 'Attributes Diff'        
        cnt1 = 0
        print('')
        #self.logit(INFO, '         Column Attribute Comparison in progress...')
        for sRow in Srows:
            sTableName   = sRow[0]
            sOrdinalPos  = sRow[1]
            sColumnName  = sRow[2]
            sColumnDflt  = sRow[3]
            sIsNull      = sRow[4]
            sDataType    = sRow[5]
            sCharMaxLen  = sRow[6]
            sNumPrecRadx = sRow[7]
            sNumScale    = sRow[8]
            sIsIdentity  = sRow[9]
            sIsGenerated = sRow[10]
           
            if sTableName != 't1':
              continue
              
            # find the index of corresponding target table            
            try:
	        x = [ x[0] for x in Trows].index(sTableName)
	    except ValueError:
	        # not found, so just ignore
	        #print ('bypassing table not found: %s' % sTableName)
	        continue
            tTableName  = Trows[x][0]	        
            #print ('found table match (%s/%s) in target array' % (sTableName, tTableName))	        
            
            cnt2 = x
            while(True):
               if cnt2 == TargetRows:
                   break;
               tTableName  = Trows[cnt2][0]
               tColumnName = Trows[cnt2][2]
               #print ('tables (%s/%s) columns (%s/%s)' % (sTableName, tTableName, sColumnName, tColumnName))
               if tTableName != sTableName:
                   # finished processing this table
                   #print('finished with this table')
                   break;
               if sColumnName == tColumnName:
                   # found match, compare attributes
                   tOrdinalPos  = Trows[cnt2][1]
                   tColumnDflt  = Trows[cnt2][3]
                   tIsNull      = Trows[cnt2][4]
                   tDataType    = Trows[cnt2][5]
                   tCharMaxLen  = Trows[cnt2][6]
                   tNumPrecRadx = Trows[cnt2][7]
                   tNumScale    = Trows[cnt2][8]
                   tIsIdentity  = Trows[cnt2][9]
                   tIsGenerated = Trows[cnt2][10]    
                   #print('column (%s) IsNull(%s/%s)' % (tColumnName, sIsNull, tIsNull))
                   
                   if sOrdinalPos != tOrdinalPos:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Ordinal Position mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sOrdinalPos, tOrdinalPos))
                   if sColumnDflt != tColumnDflt:
                       # remove schema names in column default. For instance nextval() points to a specific schema.sequence name
                       sBuffer = sColumnDflt.replace(self.Sschema + '.', '');
                       tBuffer = tColumnDflt.replace(self.Tschema + '.', '');
                       if sBuffer != tBuffer:
                           self.ddldiffs = self.ddldiffs + 1
                           self.logit (DIFF, '%20s: Table (%35s) Default mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sColumnDflt, tColumnDflt))
                   if sIsNull != tIsNull:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Is Nullable mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sIsNull, tIsNull))
                   if sDataType != tDataType:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Data Type mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sDataType, tDataType))
                   if sCharMaxLen != tCharMaxLen:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Char Max Len mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sCharMaxLen, tCharMaxLen))
                   if sNumPrecRadx != tNumPrecRadx:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Numeric Precision Radix mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sNumPrecRadx, tNumPrecRadx))
                   if sNumScale != tNumScale:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Numeric Scale mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sNumScale, tNumScale))
                   if sIsIdentity != tIsIdentity:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Is Identity mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sIsIdentity, tIsIdentity))
                   if sIsGenerated != tIsGenerated:
                       self.ddldiffs = self.ddldiffs + 1
                       self.logit (DIFF, '%20s: Table (%35s) Is Generated mismatch for column (%s) %s<>%s' % (typediff, sTableName, sColumnName, sIsGenerated, tIsGenerated))                   
               cnt2 = cnt2 + 1
            cnt1 = cnt1 + 1
            
        print ('')
        return RC_OK



    ##############################
    # Phase 4: Key/Indexes Diffs #
    ##############################
    def CompareKeysIndexes(self):    

        # We use pg_constraints to compare constraints only (UNIQUE, CHECK, PKEYS, FKEYS).  That leaves out indexes which are done later.    
        # compare constraints
        aschema = self.Sschema
        sql = "SELECT c1.relname tablename, co.conname constraintname, " \
	      "CASE WHEN co.contype = 'c' THEN 'CHECK CONSTRAINT' WHEN co.contype = 'f' THEN 'FOREIGN KEY' WHEN co.contype = 'p' THEN 'PRIMARY KEY' WHEN co.contype = 'u' THEN 'UNIQUE CONSTRAINT' WHEN co.contype = 't' THEN 'TRIGGER' WHEN co.contype = 'x' THEN 'EXCLUSION CONSTRAINT' END contype, " \
	      "CASE WHEN co.confupdtype = 'a' THEN 'NO ACTION' WHEN co.confupdtype = 'r' THEN 'RESTRICT' WHEN co.confupdtype = 'c' THEN 'CASCADE' WHEN co.confupdtype = 'n' THEN 'SET NULL' WHEN co.confupdtype = 'd' THEN 'SET DEFAULT' END confupdtype, " \
  	      "CASE WHEN co.confdeltype = 'a' THEN 'NO ACTION' WHEN co.confdeltype = 'r' THEN 'RESTRICT' WHEN co.confdeltype = 'c' THEN 'CASCADE' WHEN co.confdeltype = 'n' THEN 'SET NULL' WHEN co.confdeltype = 'd' THEN 'SET DEFAULT' END confdeltype, " \
	      "CASE WHEN co.confmatchtype = 'f' THEN 'FULL' WHEN co.confmatchtype = 'p' THEN 'PARTIAL' WHEN co.confmatchtype = 's' THEN 'SIMPLE' END confmatchtype, " \
	      "co.conkey, co.confkey, pg_get_constraintdef(co.oid), string_agg(con.column_name, ',' ORDER BY co.conkey) columns " \
	      "FROM pg_constraint co JOIN pg_namespace n ON (co.connamespace = n.oid) JOIN pg_class c1 ON (co.conrelid = c1.oid AND n.oid = c1.relnamespace) " \
	      "LEFT JOIN information_schema.constraint_column_usage con ON co.conname = con.constraint_name AND n.nspname = con.constraint_schema " \
	      "LEFT JOIN pg_attribute a ON (a.attrelid = c1.oid AND a.attname = con.column_name) " \
              "WHERE n.nspname = '%s' GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY c1.relname, co.conname" % aschema
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Constraints Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="Source Constraints Diff Error: No rows returned."
            self.logit(WARN, msg)

        aschema = self.Tschema
        sql = "SELECT c1.relname tablename, co.conname constraintname, " \
	      "CASE WHEN co.contype = 'c' THEN 'CHECK CONSTRAINT' WHEN co.contype = 'f' THEN 'FOREIGN KEY' WHEN co.contype = 'p' THEN 'PRIMARY KEY' WHEN co.contype = 'u' THEN 'UNIQUE CONSTRAINT' WHEN co.contype = 't' THEN 'TRIGGER' WHEN co.contype = 'x' THEN 'EXCLUSION CONSTRAINT' END contype, " \
	      "CASE WHEN co.confupdtype = 'a' THEN 'NO ACTION' WHEN co.confupdtype = 'r' THEN 'RESTRICT' WHEN co.confupdtype = 'c' THEN 'CASCADE' WHEN co.confupdtype = 'n' THEN 'SET NULL' WHEN co.confupdtype = 'd' THEN 'SET DEFAULT' END confupdtype, " \
  	      "CASE WHEN co.confdeltype = 'a' THEN 'NO ACTION' WHEN co.confdeltype = 'r' THEN 'RESTRICT' WHEN co.confdeltype = 'c' THEN 'CASCADE' WHEN co.confdeltype = 'n' THEN 'SET NULL' WHEN co.confdeltype = 'd' THEN 'SET DEFAULT' END confdeltype, " \
	      "CASE WHEN co.confmatchtype = 'f' THEN 'FULL' WHEN co.confmatchtype = 'p' THEN 'PARTIAL' WHEN co.confmatchtype = 's' THEN 'SIMPLE' END confmatchtype, " \
	      "co.conkey, co.confkey, pg_get_constraintdef(co.oid), string_agg(con.column_name, ',' ORDER BY co.conkey) columns " \
	      "FROM pg_constraint co JOIN pg_namespace n ON (co.connamespace = n.oid) JOIN pg_class c1 ON (co.conrelid = c1.oid AND n.oid = c1.relnamespace) " \
	      "LEFT JOIN information_schema.constraint_column_usage con ON co.conname = con.constraint_name AND n.nspname = con.constraint_schema " \
	      "LEFT JOIN pg_attribute a ON (a.attrelid = c1.oid AND a.attname = con.column_name) " \
              "WHERE n.nspname = '%s' GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY c1.relname, co.conname" % aschema
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Constraints Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Trows = self.curT.fetchall()
        if len(Trows) == 0:
            msg="Target Constraints Diff Error: No rows returned."
            self.logit(WARN, msg)

        # compare on tablename, constraintname
        typediff = 'Constraints Diff:'
        for sRow in Srows:
            sTableName      = sRow[0]
            sConstraintName = sRow[1]
            sConType        = sRow[2]
            sConfUpdType    = sRow[3] 
            sConfDelType    = sRow[4] 
            sConfMatchType  = sRow[5] 
            sConKey         = sRow[6]
            sConfKey        = sRow[7] 
            sConstraintDef  = sRow[8] 
            sColumns        = sRow[9] 
            bFoundTable = False
            bFoundConstraint = False
            for tRow in Trows:
                tTableName      = tRow[0]
                tConstraintName = tRow[1]
                tConType        = tRow[2]
                tConfUpdType    = tRow[3] 
                tConfDelType    = tRow[4] 
                tConfMatchType  = tRow[5] 
                tConKey         = tRow[6]
                tConfKey        = tRow[7] 
                tConstraintDef  = tRow[8] 
                tColumns        = tRow[9]
                # print ('stable=%s  ttable=%s' % (sTableName, tTableName))
                if sTableName == tTableName:
                    bFoundTable = True
                    # print ('sconstraint=%s  tconstraint=%s' % (sConstraintName, tConstraintName))
                    if sConstraintName == tConstraintName:
                        bFoundConstraint = True
                        if sConType != tConType:
                            msg = '%20s   Constraint Type mismatch (%s<>%s)' % (typediff, sConType, tConType)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)                               
                        if sConfUpdType != tConfUpdType:
                            msg = '%20s       ConfUpdType mismatch (%s<>%s)' % (typediff, sConfUpdType, tConfUpdType)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)     
                        if sConfDelType != tConfDelType:
                            msg = '%20s       ConfDelType mismatch (%s<>%s)' % (typediff, sConfDelType, tConfDelType)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)                                 
                        if sConfMatchType != tConfMatchType:
                            msg = '%20s     ConfMatchType mismatch (%s<>%s)' % (typediff, sConfMatchType, tConfMatchType)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)     
                        if sConKey != tConKey:
                            msg = '%20s            ConKey mismatch (%s<>%s)' % (typediff, sConKey, tConKey)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)     
                        if sConfKey != tConfKey:
                            msg = '%20s           ConfKey mismatch (%s<>%s)' % (typediff, sConfKey, tConfKey)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)     
                        if sConstraintDef != tConstraintDef:
                            # first try to remove schema qualifications and see if they are still not equal
                            # eg: (FOREIGN KEY (id) REFERENCES sample.person(id)  <>  FOREIGN KEY (id) REFERENCES sample_clone1.person(id))
                            buffer = tConstraintDef.replace(self.Tschema + '.', self.Sschema + '.')
                            if sConstraintDef != buffer:
                                msg = '%20s ConstraintDef mismatch (%s<>%s)' % (typediff, sConstraintDef, tConstraintDef)
                                self.ddldiffs = self.ddldiffs + 1
                                self.logit(DIFF, msg)                                 
                    else:
                        continue
                else:
                    continue
                if not bFoundConstraint:
                    msg = '%20s Target constraint name not found. Table(%35s)  Constraint(%s)' % (typediff, sTableName, sConstraintName)
                    self.ddldiffs = self.ddldiffs + 1
                    self.logit(DIFF, msg)                               
            if not bFoundTable:
                msg =     '%20s Target constraint table not found. Table(%35s)  Missing at least one constraint:%s-%s' % (typediff, sTableName, sConType, sConstraintName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)                               
                
        # Now just see if tablename/constraintname pairs are not found in source when compared from target.
        for tRow in Trows:
            tTableName      = tRow[0]
            tConstraintName = tRow[1]        
            bFoundTable = False
            bFoundConstraint = False
            for sRow in Srows:
                sTableName      = sRow[0]
                sConstraintName = sRow[1]
                if bFoundTable and sTableName != tTableName:
                    bFoundConstraint = False
                    break                
                if sTableName == tTableName:
                    bFoundTable = True
                    if sConstraintName == tConstraintName:
                        bFoundConstraint = True
                        break
                    else:
                        bFoundConstraint = False
                        continue
            if not bFoundConstraint:                
                msg = '%20s  Source constraint name not found. Table(%35s)  Constraint(%s)' % (typediff, tTableName, tConstraintName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)                                   
            if not bFoundTable:
                msg = '%20s Source constraint table not found. Table(%35s)  Missing at least one constraint:%s-%s' % (typediff, tTableName, tConType, tConstraintName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)
    
        # Now do INDEX checks
        aschema = self.Sschema
        sql = "SELECT c.relname AS tablename, i.relname AS indexname, x.indnatts natts, x.indnkeyatts nkeyatts, x.indisunique isunique, x.indisprimary isprimary, x.indisexclusion isexclusion, " \
              "x.indimmediate isimmediate, x.indisclustered isclustered, x.indisvalid isvalid, x.indisready isready, x.indislive islive, x.indkey, " \
	      "array_to_string(ARRAY(SELECT pg_get_indexdef(i.oid, k + 1, true) FROM generate_subscripts(x.indkey, 1) as k ORDER BY k), ',') keycols, pg_get_indexdef(i.oid) AS indexdef " \
              "FROM ((((pg_index x JOIN pg_class c ON ((c.oid = x.indrelid))) JOIN pg_class i ON ((i.oid = x.indexrelid))) " \
              "LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace))) LEFT JOIN pg_tablespace t ON ((t.oid = i.reltablespace))) " \
              "WHERE n.nspname = '%s' AND ((c.relkind = 'r'::""char"") AND (i.relkind = 'i'::""char"")) order by 1,2" % aschema
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Indexes Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="Source Indexes Diff Error: No rows returned."
            self.logit(WARN, msg)

        aschema = self.Tschema
        sql = "SELECT c.relname AS tablename, i.relname AS indexname, x.indnatts natts, x.indnkeyatts nkeyatts, x.indisunique isunique, x.indisprimary isprimary, x.indisexclusion isexclusion, " \
              "x.indimmediate isimmediate, x.indisclustered isclustered, x.indisvalid isvalid, x.indisready isready, x.indislive islive, x.indkey, " \
	      "array_to_string(ARRAY(SELECT pg_get_indexdef(i.oid, k + 1, true) FROM generate_subscripts(x.indkey, 1) as k ORDER BY k), ',') keycols, pg_get_indexdef(i.oid) AS indexdef " \
              "FROM ((((pg_index x JOIN pg_class c ON ((c.oid = x.indrelid))) JOIN pg_class i ON ((i.oid = x.indexrelid))) " \
              "LEFT JOIN pg_namespace n ON ((n.oid = c.relnamespace))) LEFT JOIN pg_tablespace t ON ((t.oid = i.reltablespace))) " \
              "WHERE n.nspname = '%s' AND ((c.relkind = 'r'::""char"") AND (i.relkind = 'i'::""char"")) order by 1,2" % aschema
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Indexes Diff Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Trows = self.curT.fetchall()
        if len(Trows) == 0:
            msg="Target Indexes Diff Error: No rows returned."
            self.logit(WARN, msg)
        
        # compare on tablename, indexname
        typediff = 'Indexes Diff:'
        for sRow in Srows:
            sTableName    = sRow[0]
            sIndexName    = sRow[1]
            sNatts        = sRow[2]
            sKeyAtts      = sRow[3]
            sIsUnique     = sRow[4]
            sIsPrimary    = sRow[5]
            sIsExclusion  = sRow[6]
            sIsImmediate  = sRow[7]
            sIsClustered  = sRow[8]
            sIsValid      = sRow[9]
            sIsReady      = sRow[10]
            sIsLive       = sRow[11]
            sIndKey       = sRow[12]
            sKeyCols      = sRow[13]
            sIndexDef     = sRow[14]
            bFoundTable   = False
            bFoundIndex   = False
            for tRow in Trows:
                tTableName    = tRow[0]
                tIndexName    = tRow[1]
                tNatts        = tRow[2]
                tKeyAtts      = tRow[3]
                tIsUnique     = tRow[4]
                tIsPrimary    = tRow[5]
                tIsExclusion  = tRow[6]
                tIsImmediate  = tRow[7]
                tIsClustered  = tRow[8]
                tIsValid      = tRow[9]
                tIsReady      = tRow[10]
                tIsLive       = tRow[11]
                tIndKey       = tRow[12]
                tKeyCols      = tRow[13]
                tIndexDef     = tRow[14]
                if sTableName == tTableName:
                    bFoundTable = True
                    # print ('sconstraint=%s  tconstraint=%s' % (sConstraintName, tConstraintName))
                    if sIndexName == tIndexName:
                        bFoundIndex = True                
                        if sNatts != tNatts:
                            msg = '%20s    Index IndNatts mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sNatts, tNatts)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sKeyAtts != tKeyAtts:
                            msg = '%20s     Index KeyAtts mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sKeyAtts, tKeyAtts)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsUnique != tIsUnique:
                            msg = '%20s    Index IsUnique mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsUnique, tIsUnique)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsPrimary != tIsPrimary:
                            msg = '%20s   Index IsPrimary mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsPrimary, tIsPrimary)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsExclusion != tIsExclusion:
                            msg = '%20s Index IsExclusion mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, ssIsExclusion, tsIsExclusion)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsClustered != tIsClustered:
                            msg = '%20s Index IsClustered mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsClustered, tIsClustered)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsValid != tIsValid:
                            msg = '%20s     Index IsValid mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsValid, tIsValid)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsReady != tIsReady:
                            msg = '%20s     Index IsReady mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsReady, tIsReady)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIsLive != tIsLive:
                            msg = '%20s      Index IsLive mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIsLive, tIsLive)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIndKey != tIndKey:
                            msg = '%20s      Index IndKey mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIndKey, tIndKey)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sKeyCols != tKeyCols:
                            msg = '%20s     Index KeyCols mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sKeyCols, tKeyCols)
                            self.ddldiffs = self.ddldiffs + 1
                            self.logit(DIFF, msg)
                        if sIndexDef != tIndexDef:
                            # first try to remove schema qualifications and see if they are still not equal
                            buffer = tIndexDef.replace(self.Tschema + '.', self.Sschema + '.')                            
                            if sIndexDef != buffer:
                                msg = '%20s Index IndexDef mismatch for table(%35s) index(%s): (%s<>%s)' % (typediff, sTableName, sIndexName, sIndexDef, tIndexDef)
                                self.ddldiffs = self.ddldiffs + 1
                                self.logit(DIFF, msg)
                else:
                        continue
            else:
                continue
            if not bFoundIndex:
                msg = '%20s       Target index name not found. Table(%35s)  Index(%s)' % (typediff, sTableName, sIndexName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)                               
        if not bFoundTable:
            msg = '%20s          Target index table not found. Table(%35s).  Missing at least one index:%s' % (typediff, sTableName, sIndexName)
            self.ddldiffs = self.ddldiffs + 1
            self.logit(DIFF, msg)                                       
        
        # Now just see if tablename/indexname pairs are not found in source when compared from target.        
        LastTable = ''
        for tRow in Trows:
            tTableName = tRow[0]
            tIndexName = tRow[1]        
            bFoundTable = False
            bFoundIndex = False
            for sRow in Srows:
                sTableName = sRow[0]
                sIndexName = sRow[1]
                if bFoundTable and sTableName != tTableName:
                    bFoundIndex = False
                    break
                if sTableName == tTableName:
                    bFoundTable = True
                    if sIndexName == tIndexName:
                        bFoundIndex = True
                        break
                    else:
                        bFoundIndex = False
                        continue
            if not bFoundIndex and bFoundTable:                
                msg = '%20s       Source index name not found. Table(%35s)  Index(%s)' % (typediff, tTableName, tIndexName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)                                   
            if not bFoundTable:
                msg = '%20s      Source index table not found. Table(%35s)  Missing at least one index:%s' % (typediff, tTableName, tIndexName)
                self.ddldiffs = self.ddldiffs + 1
                self.logit(DIFF, msg)        
    
        print ('')
        return RC_OK        
    
    ############################
    # Phase 5: Row count diffs #
    ############################
    def CompareRowCounts(self):    
        aschema = self.Sschema
        sql = "SELECT a.tblname, a.rowcnt, b.tblname, b.rowcnt from " \
    	      "(SELECT c.relname as tblname, c.reltuples::bigint as rowcnt from pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and n.nspname = '%s' and c.relkind = 'r' ORDER BY 1) a,  " \
              "(SELECT t.relname as tblname, t.n_live_tup::bigint as rowcnt from pg_stat_user_tables t, pg_class c, pg_namespace n WHERE n.nspname = '%s' AND  " \
              "n.oid = c.relnamespace AND n.nspname = t.schemaname and t.relname = c.relname and c.relkind = 'r'  ORDER BY 1) b WHERE a.tblname = b.tblname" % (aschema, aschema)
        try:              
            self.curS.execute(sql)
        except Exception as error:
            msg="Source Table Row Counts Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR

        Srows = self.curS.fetchall()
        if len(Srows) == 0:
            msg="Source Row Counts Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    
    
        '''
        SELECT a.tblname, a.rowcnt, b.tblname, b.rowcnt from 
    	(SELECT c.relname as tblname, c.reltuples::bigint as rowcnt from pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and n.nspname = 'sample_clone1' and c.relkind = 'r' ORDER BY 1) a, 
        (SELECT t.relname as tblname, t.n_live_tup::bigint as rowcnt from pg_stat_user_tables t, pg_class c, pg_namespace n WHERE n.nspname = 'sample_clone1' AND 
        n.oid = c.relnamespace AND n.nspname = t.schemaname and t.relname = c.relname and c.relkind = 'r'  ORDER BY 1) b 
        WHERE a.tblname = b.tblname;
        '''
     
        aschema = self.Tschema
        sql = "SELECT a.tblname, a.rowcnt, b.tblname, b.rowcnt from " \
    	      "(SELECT c.relname as tblname, c.reltuples::bigint as rowcnt from pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and n.nspname = '%s' and c.relkind = 'r' ORDER BY 1) a,  " \
              "(SELECT t.relname as tblname, t.n_live_tup::bigint as rowcnt from pg_stat_user_tables t, pg_class c, pg_namespace n WHERE n.nspname = '%s' AND  " \
              "n.oid = c.relnamespace AND n.nspname = t.schemaname and t.relname = c.relname and c.relkind = 'r'  ORDER BY 1) b WHERE a.tblname = b.tblname" % (aschema, aschema)
        try:              
            self.curT.execute(sql)
        except Exception as error:
            msg="Target Table Row Counts Error %s *** %s" % (type(error), error)
            self.logit(ERR, msg)
            return RC_ERR
    
        Trows = self.curT.fetchall()
        if len(Trows) == 0:
            msg="Target Row Counts Diff Error: No rows returned."
            self.logit(ERR, msg)
            return RC_ERR    

        cnt1 = 0
        diffs = 0
        typediff = 'Row Counts Diff:'
        for sRow in Srows:
            sTable1      = sRow[0]
            sCount1      = sRow[1]
            sTable2      = sRow[2]
            sCount2      = sRow[3]
            if sTable1 != sTable2:
                msg='Program Errror: unexpected table mismatch for source tables (%s, %s)' % (sTable1, sTable2)
                self.logit(ERR, msg)
                return RC_ERR                

            cnt2 = 0
            for tRow in Trows:
                tTable1      = tRow[0]
                tCount1      = tRow[1]
                tTable2      = tRow[2]
                tCount2      = tRow[3]
                if tTable1 != tTable2:
                    msg='Program Errror: unexpected table mismatch for target tables (%s, %s)' % (tTable1, tTable2)
                    self.logit(ERR, msg)
                    return RC_ERR     

                if sTable1 == tTable1:
                    # we are using pg_class.reltuples not pg_stat_user_tables.n_live_tup
                    if sCount1 != tCount1:
                        # Before giving up, do the real count if detailescan is indicated.
                        if pg.scantype != 'detailedscan':
                            diffs = diffs + 1
                            self.rowcntdiffs = self.rowcntdiffs + 1
                            self.logit (DIFF, '%20s %-35s rowcnts mismatch %08d<>%08d' % (typediff, sTable1, sCount1, tCount1))
                        else:
                            aschema = self.Sschema
                            sql = 'SELECT COUNT(*) from %s."%s"' % (aschema, sTable1)
                            try:              
                                self.curS.execute(sql)
                            except Exception as error:
                                msg="Source Table Real Row Counts Error %s *** %s" % (type(error), error)
                                self.logit(ERR, msg)
                                return RC_ERR
                            Srow = self.curS.fetchone()
                            aschema = self.Tschema
                            sql = 'SELECT COUNT(*) from %s."%s"' % (aschema, tTable1)
                            try:              
                                self.curT.execute(sql)
                            except Exception as error:
                                msg="Target Table Real Row Counts Error %s *** %s" % (type(error), error)
                                self.logit(ERR, msg)
                                return RC_ERR
                            Trow = self.curT.fetchone()
                            if Srow[0] != Trow[0]:
                                diffs = diffs + 1
                                self.rowcntdiffs = self.rowcntdiffs + 1
                                self.logit (DIFF, '%20s %-35s Real rowcnts mismatch %08d<>%08d' % (typediff, sTable1, Srow[0], Trow[0]))

        print ('')
        return RC_OK    


def setupOptionParser():
    parser = OptionParser(add_help_option=False,   description=DESCRIPTION)
    
    parser.add_option("-H", "--Shost",     dest="shost",     help="Source host",     default="",metavar="SOURCEHOST")
    parser.add_option("-P", "--Sport",     dest="sport",     help="Source port",     default=5432,metavar="SOURCEPORT", type=int)
    parser.add_option("-U", "--Suser",     dest="suser",     help="Source user",     default="",metavar="SOURCEUSER")
    parser.add_option("-D", "--Sdb",       dest="sdb",       help="Source database", default="",metavar="SOURCEDB")
    parser.add_option("-S", "--Sschema",   dest="sschema",   help="Source schema",   default="",metavar="SOURCESCHEMA")
    parser.add_option("-h", "--Thost",     dest="thost",     help="Target host",     default="",metavar="TARGETHOST")
    parser.add_option("-p", "--Tport",     dest="tport",     help="Target port",     default=5432,metavar="TARGETPORT", type=int)
    parser.add_option("-u", "--Tuser",     dest="tuser",     help="Target user",     default="",metavar="TARGETUSER")
    parser.add_option("-d", "--Tdb",       dest="tdb",       help="Target database", default="",metavar="TARGETDB")
    parser.add_option("-s", "--Tschema",   dest="tschema",   help="Target schema",   default="",metavar="TARGETSCHEMA")    

    parser.add_option("-t", "--scantype", dest="scantype",   help="scantype [SimpleScan | DetailedScan]", default="",metavar="SCANTYPE")
    parser.add_option("-l", "--log",      dest="logging",    help="log diffs to output file",default=False, action="store_true")
    parser.add_option("-v", "--verbose", dest="verbose",     help="Verbose Output",default=False, action="store_true")
    
    parser.add_option("-r", "--ignore_rowcounts", dest="ignore_rowcounts",  help="Ignore row counts diffs",default=False, action="store_true")
    parser.add_option("-i", "--ignore_indexes",   dest="ignore_indexes",    help="Ignore index diffs",default=False, action="store_true")

    return parser

####################
# MAIN ENTRY POINT #
####################

dt_started = datetime.datetime.utcnow()
optionParser   = setupOptionParser()
(options,args) = optionParser.parse_args()

# get the class instantiation
pg = maint()

# Get parms
pg.Shost             = options.shost
pg.Sport             = options.sport
pg.Suser             = options.suser
pg.Sdb               = options.sdb
pg.Sschema           = options.sschema
pg.Thost             = options.thost
pg.Tport             = options.tport
pg.Tuser             = options.tuser
pg.Tdb               = options.tdb
pg.Tschema           = options.tschema
pg.scantype          = (options.scantype).lower()
pg.logging           = options.logging
pg.verbose           = options.verbose
pg.IgnoreRowCounts   = options.ignore_rowcounts
pg.IgnoreIndexes     = options.ignore_indexes

# check parms
if pg.Suser == '':
     print ('Source DBuser not provided.')
     sys.exit(FAIL)    
elif pg.Tuser == '':
     print ('Target DBuser not provided.')
     sys.exit(FAIL)    
elif pg.Sdb == '':
     print ('Source DB not provided.')
     sys.exit(FAIL)    
elif pg.Tdb == '':
     print ('Target DB not provided.')
     sys.exit(FAIL)    
elif pg.Sschema == '':     
     print ('Source schema not provided.')
     sys.exit(FAIL)         
elif pg.Tschema == '':     
     print ('Target schema not provided.')
     sys.exit(FAIL)              
elif pg.scantype != 'simplescan' and pg.scantype != 'detailedscan':     
     print ('Scantype invalid: %s.  Must be "SimpleScan" or "DetailedScan"' % pg.scantype)
     sys.exit(FAIL)              

print ('%s  Version %.1f  %s  Compare in progress...' % (PROGNAME, VERSION, ADATE))
     
     
# capture database warnings like data truncated warnings 
# this doesnt work anymore!!!! Fix later 
#warnings.filterwarnings('error', category=pgdb.Warning)

# open this script's log file
now = datetime.datetime.now().strftime("%Y_%m_%d")
pg.logit(INFO,"--------- program start ----------")

# get connection handles to source and target schemas
rc = pg.ConnectAll() 
if rc == RC_ERR:
    # error has already been logged
    pg.logit(INFO, 'Program ended with error(s).')
    pg.CloseStuff()
    sys.exit(FAIL)

#pg.logit(INFO, "connected to source and target databases successfully.")

# Phase 1: Compare object counts
pg.logit(INFO, "PHASE 1: Comparing Object Counts...")
rc = pg.CompareObjects()
if rc == RC_ERR:
    # error has already been logged
    pg.logit(INFO, 'CompareObjects() Errror.')
    pg.CloseStuff()
    sys.exit(FAIL)

# Phase 2: Compare Tables/Views
pg.logit(INFO, "PHASE 2: Comparing Tables/Views...")
rc = pg.CompareTablesViews()
if rc == RC_ERR:
    # error has already been logged
    pg.logit(INFO, 'CompareTablesViews() Errror.')
    pg.CloseStuff()
    sys.exit(FAIL)

# Phase 3: Compare Columns
pg.logit(INFO, "PHASE 3: Comparing Columns...")
rc = pg.CompareColumns()
if rc == RC_ERR:
    # error has already been logged
    pg.logit(INFO, 'CompareColumns() Errror.')
    pg.CloseStuff()
    sys.exit(FAIL)    

# Phase 4: Compare Key/Indexes
if pg.IgnoreIndexes:
    pg.logit(INFO, 'Bypassing Index comparison...')
else:
    pg.logit(INFO, "PHASE 4: Comparing Constraints/Indexes...")
    rc = pg.CompareKeysIndexes()
    if rc == RC_ERR:
        # error has already been logged
        pg.logit(INFO, 'CompareKeysIndexes Errror.')
        pg.CloseStuff()
        sys.exit(FAIL)    

# Phase 5: Compare Row Counts
if pg.IgnoreRowCounts:
    pg.logit(INFO, 'Bypassing Row Count comparison...')
else:
    pg.logit(INFO, "PHASE 5: Comparing Row Counts...")
    if pg.IgnoreRows_scantype == 'simplescan':
        pg.logit(WARN, '*** SimpleScan: Row Counts are statistically computed so make sure you run ANALYZE beforehand. ***')
    rc = pg.CompareRowCounts()
    if rc == RC_ERR:
        # error has already been logged
        pg.logit(INFO, 'CompareRowCounts() Errror.')
        pg.CloseStuff()
        sys.exit(FAIL)    

dt_ended = datetime.datetime.utcnow()
secs = round((dt_ended - dt_started).total_seconds())

if pg.scantype == 'simplescan':
    if pg.ddldiffs == 0:
        pg.logit(INFO,"Summary (%d seconds): No DDL differences found." % secs)
    else:
        pg.logit(INFO,"Summary (%d seconds): DDL Differences found: %d" % (secs, pg.ddldiffs))
else:
    if pg.ddldiffs == 0 and pg.rowcntdiffs == 0:
        pg.logit(INFO,"Summary (%d seconds): No DDL differences found." % secs)
    else:
        pg.logit(INFO,"Summary (%d seconds): DDL Differences found: ddl (%d)  rowcnts (%d)" % (secs, pg.ddldiffs, pg.rowcntdiffs))

pg.logit(INFO,"--------- program end   ----------")
pg.CloseStuff()

sys.exit(SUCCESS)

''' 
following is stuff to get other DDL stuff to compare
TRIGGERS:
SELECT trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_catalog, event_object_schema, event_object_table, action_statement 
FROM information_schema.triggers WHERE trigger_schema = 'public'  ORDER BY trigger_schema, event_object_table, trigger_name limit 10;

select n.nspname as schema_name, c.relname as table_name, t.tgname as trigger_name 
FROM pg_trigger t, pg_class c, pg_namespace n where n.nspname = 'public' and  t.tgrelid = c.oid and c.relnamespace = n.oid order by 1,2,3 limit 10;

select pg_get_triggerdef(oid) from pg_trigger where tgname = '<your trigger name>';

VIEWS: 
SELECT schemaname, viewname, definition  FROM pg_catalog.pg_views

SELECT a.tblname, a.rowcnt, b.tblname, b.rowcnt from (SELECT c.relname as tblname, c.reltuples::bigint as rowcnt from pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and n.nspname = 'sample' and c.relkind = 'r' ORDER BY 1) a, 
(SELECT t.relname as tblname, t.n_live_tup::bigint as rowcnt from pg_stat_user_tables t, pg_class c, pg_namespace n WHERE n.nspname = 'sample' AND 
n.oid = c.relnamespace AND n.nspname = t.schemaname and t.relname = c.relname and c.relkind = 'r'  ORDER BY 1) b WHERE a.tblname = b.tblname and a.tblname in ('measurement');
SELECT a.tblname, a.rowcnt, b.tblname, b.rowcnt from (SELECT c.relname as tblname, c.reltuples::bigint as rowcnt from pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and n.nspname = 'sample_clonedata' and c.relkind = 'r' ORDER BY 1) a, 
(SELECT t.relname as tblname, t.n_live_tup::bigint as rowcnt from pg_stat_user_tables t, pg_class c, pg_namespace n WHERE n.nspname = 'sample_clonedata' AND 
n.oid = c.relnamespace AND n.nspname = t.schemaname and t.relname = c.relname and c.relkind = 'r'  ORDER BY 1) b WHERE a.tblname = b.tblname and a.tblname in ('measurement');

COMMENTS:
SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname as relname, d.description as comments
FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) LEFT JOIN pg_description d ON (c.oid = d.objoid) LEFT JOIN pg_attribute a ON (c.oid = a.attrelid AND a.attnum > 0 and a.attnum = d.objsubid)
WHERE d.objsubid = 0 AND d.description IS NOT NULL AND n.nspname in ('sample')
UNION 
SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname::text as relname, d.description as comments from pg_namespace n, pg_description d, pg_class c where d.objoid = c.oid and c.relnamespace = n.oid and d.objsubid = 0 AND n.nspname in ('sample')
UNION 
-- column comments
SELECT 'COLUMN' as OBJECT, s.column_name as relname, d.description as comments FROM pg_description d, pg_class c, pg_namespace n, information_schema.columns s 
WHERE d.objsubid > 0 and d.objoid = c.oid and c.relnamespace = n.oid and n.nspname = 'sample' and s.table_schema = n.nspname and s.table_name = c.relname and s.ordinal_position = d.objsubid
UNION
-- domain comments
SELECT 'DOMAIN' as OBJECT, t.typname as relname, d.description as comments from pg_description d, pg_type t, pg_namespace n  where d.description = 'my domain comments on addr' and d.objoid = t.oid and t.typtype = 'd' and t.typnamespace = n.oid AND d.objsubid = 0 AND n.nspname = 'sample'
UNION
-- schema comments
SELECT 'SCHEMA' as OBJECT, n.nspname as relname, d.description as comments FROM pg_description d, pg_namespace n WHERE d.objsubid = 0 AND d.classoid::regclass = 'pg_namespace'::regclass AND d.objoid = n.oid and n.nspname = 'sample' 
UNION
-- types comments
SELECT 'TYPE' AS OBJECT, t.typname as relname, pg_catalog.obj_description(t.oid, 'pg_type') as comments FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR
(SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el
WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname in ('sample') AND pg_catalog.obj_description(t.oid, 'pg_type') IS NOT NULL AND t.typtype = 'c'
UNION 
SELECT 'COLATION' as OBJECT, collname as relname,  pg_catalog.obj_description(c.oid, 'pg_collation') as comments FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n WHERE n.oid = c.collnamespace AND 
c.collencoding IN (-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND n.nspname in ('sample') AND pg_catalog.obj_description(c.oid, 'pg_collation') IS NOT NULL 
UNION
SELECT CASE WHEN p.prokind = 'f' THEN 'FUNCTION' WHEN p.prokind = 'p' THEN 'PROCEDURE' WHEN p.prokind = 'a' THEN 'AGGREGATE FUNCTION' WHEN p.prokind = 'w' THEN 'WINDOW FUNCTION' END as OBJECT,
p.proname as relname, d.description as comments from pg_catalog.pg_namespace n 
JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname in ('sample')
UNION
SELECT 'POLICY' as OBJECT, p1.policyname as relname, d.description as comments from pg_policies p1, pg_policy p2, pg_class c, pg_namespace n, pg_description d WHERE d.objsubid = 0 AND p1.schemaname = n.nspname and p1.tablename = c.relname AND 
n.oid = c.relnamespace and c.relkind in ('r','p') and p1.policyname = p2.polname and d.objoid = p2.oid and p1.schemaname in ('sample')
ORDER BY 1;

WITH details as (SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname as relname, d.description as comments
FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) LEFT JOIN pg_description d ON (c.oid = d.objoid) LEFT JOIN pg_attribute a ON (c.oid = a.attrelid AND a.attnum > 0 and a.attnum = d.objsubid)
WHERE d.objsubid = 0 AND d.description IS NOT NULL AND n.nspname in ('sample')
UNION 
SELECT CASE WHEN c.relkind = 'r' THEN 'TABLE' WHEN c.relkind = 'p' THEN 'PARTITIONED TABLE' WHEN c.relkind = 'S' THEN 'SEQUENCE' WHEN c.relkind = 'f' THEN 'FOREIGN TABLE' WHEN c.relkind = 'v' THEN 'VIEW' WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW' WHEN c.relkind = 'i' THEN 'INDEX' WHEN c.relkind = 'c' THEN 'TYPE' END as OBJECT, c.relname::text as relname, d.description as comments from pg_namespace n, pg_description d, pg_class c where d.objoid = c.oid and c.relnamespace = n.oid and d.objsubid = 0 AND n.nspname in ('sample')
UNION 
-- column comments
SELECT 'COLUMN' as OBJECT, s.column_name as relname, d.description as comments FROM pg_description d, pg_class c, pg_namespace n, information_schema.columns s 
WHERE d.objsubid > 0 and d.objoid = c.oid and c.relnamespace = n.oid and n.nspname = 'sample' and s.table_schema = n.nspname and s.table_name = c.relname and s.ordinal_position = d.objsubid
UNION
-- domain comments
SELECT 'DOMAIN' as OBJECT, t.typname as relname, d.description as comments from pg_description d, pg_type t, pg_namespace n  where d.description = 'my domain comments on addr' and d.objoid = t.oid and t.typtype = 'd' and t.typnamespace = n.oid AND d.objsubid = 0 AND n.nspname = 'sample'
UNION
-- schema comments
SELECT 'SCHEMA' as OBJECT, n.nspname as relname, d.description as comments FROM pg_description d, pg_namespace n WHERE d.objsubid = 0 AND d.classoid::regclass = 'pg_namespace'::regclass AND d.objoid = n.oid and n.nspname = 'sample' 
UNION
-- types comments
SELECT 'TYPE' AS OBJECT, t.typname as relname, pg_catalog.obj_description(t.oid, 'pg_type') as comments FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace WHERE (t.typrelid = 0 OR
(SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el
WHERE el.oid = t.typelem AND el.typarray = t.oid) AND n.nspname in ('sample') AND pg_catalog.obj_description(t.oid, 'pg_type') IS NOT NULL AND t.typtype = 'c'
UNION 
SELECT 'COLATION' as OBJECT, collname as relname,  pg_catalog.obj_description(c.oid, 'pg_collation') as comments FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n WHERE n.oid = c.collnamespace AND 
c.collencoding IN (-1, pg_catalog.pg_char_to_encoding(pg_catalog.getdatabaseencoding())) AND n.nspname in ('sample') AND pg_catalog.obj_description(c.oid, 'pg_collation') IS NOT NULL 
UNION
SELECT CASE WHEN p.prokind = 'f' THEN 'FUNCTION' WHEN p.prokind = 'p' THEN 'PROCEDURE' WHEN p.prokind = 'a' THEN 'AGGREGATE FUNCTION' WHEN p.prokind = 'w' THEN 'WINDOW FUNCTION' END as OBJECT,
p.proname as relname, d.description as comments from pg_catalog.pg_namespace n 
JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid JOIN pg_description d ON (d.objoid = p.oid) WHERE d.objsubid = 0 AND n.nspname in ('sample')
UNION
SELECT 'POLICY' as OBJECT, p1.policyname as relname, d.description as comments from pg_policies p1, pg_policy p2, pg_class c, pg_namespace n, pg_description d WHERE d.objsubid = 0 AND p1.schemaname = n.nspname and p1.tablename = c.relname AND 
n.oid = c.relnamespace and c.relkind in ('r','p') and p1.policyname = p2.polname and d.objoid = p2.oid and p1.schemaname in ('sample')
ORDER BY 1)
Select object, count(*) from details group by 1 order by 1;


'''

