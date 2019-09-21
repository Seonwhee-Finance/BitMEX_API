import os
import argparse
import subprocess
import sys
import boto3

class MySQLDump:

    def __init__(self, options):
        self.username = options.username
        self.password = options.password
        self.outdir   = options.outdir
        self.prefix   = options.prefix
        self.ignore_databases = ['information_schema', 'performance_schema', 'mysql', 'sys']

    def dump(self):
        self._check_outdir()

        databases = self._get_databases()

        for database in databases:
            if database not in self.ignore_databases:
                self._dump_database(database)

    def _check_outdir(self):
        if os.path.isdir(self.outdir) == False:
            raise Exception("Out directory '%s' not found." % self.outdir)

    def _get_databases(self):
        command = "mysql --silent %s -e 'SHOW DATABASES'" % self._get_options()
        result = subprocess.getstatusoutput(command)
        if result[0] > 0:
            raise Exception('Failed to fetch databases: ' + result[1]);

        return result[1].split("\n")

    def _dump_database(self, database):
        print("Dumping database %s..." % database)
        filename = self.outdir + '/' + self.prefix + database + '.sql.gz'
        command = "mysqldump %s %s | gzip > %s" % (self._get_options(), database, filename)
        result = subprocess.getstatusoutput(command)
        if result[0] > 0:
            raise Exception(result[1])

        print(filename)

    def _get_options(self):
        options = []

        if self.username:
            options.append("-u %s" % self.username)

        if self.password:
            options.append("-p %s" % self.password)

        return ' '.join(options)

def To_S3_storage(fileToUpload):
    s3 = boto3.client('s3')
    s3.upload_file(fileToUpload, 'sagemaker-neuralbc', fileToUpload.split('/')[-1])

def main():
    parser = argparse.ArgumentParser(description='Dump all mysql databases')
    parser.add_argument('-u', type=str, help='mysql username', default='', dest='username')
    parser.add_argument('-p', type=str, help='mysql password', default='', dest='password')
    parser.add_argument('-x', type=str, help='file prefix', default='mysql-', dest='prefix')
    parser.add_argument('outdir', type=str, help='directory to make dump files')

    args = parser.parse_args()

    try:
        mysqldump = MySQLDump(args)
        mysqldump.dump()
    except Exception as e:
        print(e)
        exit(1)

    To_S3_storage("./DBbackup/bitmex_trading.sql.gz")

if __name__ == "__main__":
    main()