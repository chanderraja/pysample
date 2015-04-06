#!/usr/bin/python3
__author__ = 'chander.raja@gmail.com'

import sys
import shutil
import subprocess
import shlex

import mysql.connector


class analysis:
    "class for analysis blocks"
    def __init__(self, cnx, type, sp_start, sp_end, sp_reset, sp_fail, perl_script, sql=""):
        self.cnx = cnx
        self.start = sp_start
        self.end = sp_end
        self.reset = sp_reset
        self.fail = sp_fail
        self.script = perl_script
        self.type = type
        self.key = 0
        self.dir = ""
        self.args=""
        self.sql = sql
        self.log_prefix=self.type + ': '

    def next_job(self):
        cursor = self.cnx.cursor()
        try:
            cursor.callproc(self.start)
        except mysql.connector.Error as err:
            print('{}Failed to get next job, DB err = {}'.format(self.log_prefix, err))
            return False

        for rows in cursor.stored_results():
            row = rows.fetchone()
            if not row:
                return False
            (self.key, self.dir, self.args, sql) = row
            if not self.sql:
                self.sql = sql
            print('{}next job = [{}]'.format(self.log_prefix, self.key))
            self.log_prefix = '{}[{}]: '.format(self.type, self.key)
            return True

        return False

    def reset(self):
        cursor = self.cnx.cursor()
        args = (self.key, )
        try:
            cursor.callproc(self.reset, args)
        except mysql.connector.Error as err:
            print('{}Failed to reset job, DB err = {}'.format(self.log_prefix, self.key, err))
            sys.exit(1)

    def fail(self):
        cursor = self.cnx.cursor()
        args = (self.key, )
        try:
            cursor.callproc(self.fail, args)
        except mysql.connector.Error as err:
            print('{}Error calling failure proc, DB err = {}'.format(self.log_prefix, self.key, err))
            sys.exit(1)

    def done(self):
        cursor = self.cnx.cursor()
        args = ( self.key, )
        try:
            cursor.callproc(self.end, args)
        except mysql.connector.Error as err:
            print('{}Failed {}, DB err ={}'.format(self.log_prefix, self.end, err))
            sys.exit(1)

    def analyze(self):
        shutil.rmtree(self.dir, True)
        cmd = self.script + ' ' + self.args
        ret = subprocess.call(shlex.split(cmd))
        if ret != 0:
            print('{} {} failed!'.format(self.log_prefix, self.type))
            return False
        else:
            print('{}{} successful!'.format(self.log_prefix, self.type))
            return True

    def execSql(self):
        if not self.sql:
            print('{}No SQL to execute'.format(self.log_prefix))
            return True

        cursor = self.cnx.cursor()
        try:
            cursor.execute(self.sql)
        except mysql.connector.Error as err:
            print('{}Failed to run sql cmd {}\n err = {}'.format(self.log_prefix, self.sql, err))
            return False
        self.cnx.commit()
        return True


    def run(self):
        if not self.next_job():
            print('{}no more jobs'.format(self.log_prefix))
            return

        if not self.analyze():
            self.fail()
            return

        self.execSql()

        self.done()



def run(cnx):
    obj = analysis(cnx,
                   'analysis',
                   'tuna.spAnalysisStart',
                   'tuna.spAnalysisEnd',
                   'tuna.spAnalysisReset',
                   'tuna.spAnalysisFail',
                   './analysis.pl')
    obj.run()


