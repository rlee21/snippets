import os
import sys
import shutil
import json
import re
import argparse
import fileinput
import tempfile
import getpass
from datetime import datetime

TEST_GW = "cg1test"
PROD_GW = "cg2wow"
HADOOP_DEPLOY_SH = "deploy2hadoop.sh"

def parse_args():
    """ Parse command line arguments """

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env",
                        help="specify the environment (i.e. cluster) to run in, \
                        e.g. test, prod. (required field)",
                        default=None, required=True, choices=['test', 'prod'])
    parser.add_argument("-n", "--namespace",
                        help="namespace to run in. Reserved keys: user, final. \
                        (required field)",
                        default=None, required=True)
    parser.add_argument("-s", "--start-time",
                        help="if specified, Oozie coordinator start time will \
                        be replaced: 'now' - current day and time overwrites \
                        previous value, 'today' - current day is set (time remains unchanged)",
                        default=None, choices=['now', 'today'])
    parser.add_argument("-r", "--running-frequency", nargs=2,
                        help="if specified, Oozie coordinator running frequency \
                        will be overwritten to the provided one. e.g.  '05 9 * * *' ",
                        default=None)
    parser.add_argument("-x", "--additional-ext", nargs='+',
                        help="if specified, we will copy all files with specified extensions",
                        default=None)
    parser.add_argument("-f", "--additional-files", nargs='+',
                        help="if specified, we will copy all extra files specified",
                        default=None)
    parser.add_argument("-d", "--additional-folders", nargs='+',
                        help="if specified, we will copy all extra folders specified",
                        default=None)
    parser.add_argument("-v", "--virtualenv",
                        help="if specified, we will setup virtual environment for python",
                        action='store_true', default=False)
    parser.add_argument("--skip-host-key-check",
                        help="if specified, host key verification will be skipped",
                        action='store_true', default=None)
    parser.add_argument("--no-scp",
                        help="Only create deploy directory locally, do not scp the deploy \
                        directory to the gateway server",
                        action='store_true', default=False)

    return parser.parse_args()

def create_dest_dir(dest_dir):
    """ Create the deploy dir """
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir, ignore_errors=True)
    os.mkdir(dest_dir)

    sys.stdout.write("Created the deployment directory: {}\n\n".format(dest_dir))

def add_properties(args, source_dir, output_file):
    """ Read properties json and create oozie properties file """
    job_path = os.path.join(source_dir, "job.properties.json")
    prop_file = open(job_path)
    prop_json = json.load(prop_file)

    for segment in prop_json:
        if (segment["environment"] == args.env or segment["environment"] == "*") and \
           (segment["namespace"] == args.namespace or segment["namespace"] == "*"):
            output_file.write("# Environment: {}, Namespace: {}\n".format(segment["environment"],
                                                                          segment["namespace"]))
            properties = segment["properties"]
            for prop in properties:
                output_file.write("{}={}\n".format(prop["name"], prop["value"]))
            output_file.write("\n")

def write_properties(args, project_dir, common_dir, dest_dir):
    """ Write the common and project properties to the oozie properties file """
    job_path = os.path.join(dest_dir, "job.properties")
    output = open(job_path, "w")

    output.write("# Common properties:\n\n")
    add_properties(args, common_dir, output)

    output.write("# Project properties:\n\n")
    add_properties(args, project_dir, output)

    output.close()

def adjust_properties(args, dest_dir):
    """
    Adjust properties files if a start time or running frequency is
    specified in the args
    """
    job_properties = os.path.join(dest_dir, 'job.properties')

    if args.start_time:
        sys.stdout.write("Adjusting coordinator start time\n")
        start_time = args.start_time
        start_time_pattern = ''
        new_start_time = r'\1='
        if start_time == 'now':
            new_start_time = new_start_time + datetime.utcnow().strftime('%Y-%m-%dT%H:%MZ')
            start_time_pattern = r'(scheduleStartTime).*'
        elif start_time == 'today':
            new_start_time = new_start_time + datetime.now().strftime('%Y-%m-%d') + r'\2'
            start_time_pattern = r'(scheduleStartTime).*(T..:..Z)'

        with open(job_properties, "r") as sources:
            lines = sources.readlines()
        with open(job_properties, "w") as sources:
            for line in lines:
                sources.write(re.sub(start_time_pattern, new_start_time, line))

    sys.stderr.write("running_freq: " + str(args.running_frequency) + "\n")
    if args.running_frequency != None:
        running_frequency = ' '.join(args.running_frequency)
        sys.stdout.write("Adjusting running frequency to: " + running_frequency + " * * *\n")
        new_start_time = r'\1=' + running_frequency + " * * *"
        start_time_pattern = r'(runningFrequency).*'

        with open(job_properties, "r") as sources:
            lines = sources.readlines()
        with open(job_properties, "w") as sources:
            for line in lines:
                sources.write(re.sub(start_time_pattern, new_start_time, line))

    sys.stdout.write("Generated job.properties file.\n\n")

def copy_files(args, project_dir, common_dir, dest_dir):
    """ Copy all needed files to the dest_dir """
    deploy2hadoopFilename = HADOOP_DEPLOY_SH
    src_path = os.path.join(common_dir, deploy2hadoopFilename)
    shutil.copy(src_path, dest_dir)
    sys.stdout.write("Copied {}\n".format(HADOOP_DEPLOY_SH))

    action_path = os.path.join(common_dir, "actionScripts")
    root, dirs, files = next(os.walk(action_path))
    for filename in files:
        full_path = os.path.join(action_path, filename)
        shutil.copy(full_path, dest_dir)
        sys.stdout.write("Copied {}\n".format(full_path))

    extensions = ["sh", "hql", "py", "xml", "conf", "csv", "R", "json", "txt"]

    def replace_in_file(filename, old, new):
        for line in fileinput.input(filename, inplace=True):
            sys.stdout.write(line.replace(old, new) + ' ')
        sys.stdout.write('\n')

    # Also update deploy2hadoop shell script to account for the additional extensions
    hadoop_deploy_path = os.path.join(dest_dir, deploy2hadoopFilename)
    if args.additional_ext:
        extensions = extensions + args.additional_ext
        replace_in_file(hadoop_deploy_path, "%ADDITIONAL_EXT%", "," + ','.join(args.additional_ext))
    if args.virtualenv:
        replace_in_file(hadoop_deploy_path, "%SETUP_VIRTUAL_PYTHON_CONFIG%", str(args.virtualenv))
    if args.additional_folders:
        replace_in_file(hadoop_deploy_path, "%ADDITIONAL_DIRS%", ';'.join(args.additional_folders))

    sys.stdout.write("Copying files (" + ', '.join(extensions) + "):\n")

    root, dirs, files = next(os.walk(project_dir))
    dot_ext = [ "." + ext for ext in extensions ]
    for f in files:
        if f != "deploy2gateway.sh" and f != "job.properties.json":
            if os.path.splitext(f)[1] in dot_ext:
                src_path = os.path.join(project_dir, f)
                shutil.copy(src_path, dest_dir)
                sys.stdout.write(src_path + '\n')
    sys.stdout.write("\n")

    sys.stdout.write("\nCopying directories:\n")
    directories = ["lib", "setup", "updates"]
    for directory in directories:
        src = os.path.join(project_dir, directory)
        dest = os.path.join(dest_dir, directory)
        if os.path.exists(src):
            shutil.copytree(src, dest)
            sys.stdout.write(src + '\n')
    
    if args.additional_folders:
        for directory in args.additional_folders:
            src = os.path.join(project_dir, directory)
            dest = os.path.join(dest_dir, directory)
            if os.path.exists(src):
                shutil.copytree(src, dest)
                sys.stdout.write(src + '\n')
    sys.stdout.write("\n")

    dest_setup_dir = os.path.join(dest_dir, "setup")
    if os.path.exists(dest_setup_dir):
        hbase_path = os.path.join(common_dir, "createHiveHBase.sh")
        shutil.copy(hbase_path, dest_setup_dir)
        sys.stdout.write("Copied createHiveHBase.sh\n")
    sys.stdout.write("\n")

    # Copy additional files specified
    if args.additional_files:
        for extraFile in args.additional_files:
            shutil.copy(os.path.abspath(extraFile), dest_dir)

def dos2unix(dir_path):
    """ Perform dos2unix for all files in the dir_path """
    root, dirs, files = next(os.walk(dir_path))
    for f in files:
        dest_file = os.path.join(dir_path, f)
        permissions = os.stat(dest_file).st_mode
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        with open(os.path.join(dir_path, f), 'r') as fh:
            for line in fh:
                tmp_file.write(bytearray(line.replace('\r\n', '\n'), 'utf-8'))

        tmp_file.close()
        shutil.move(tmp_file.name, dest_file)
        os.chmod(dest_file, permissions)

def run_scp(args, dest_dir):
    gateway = TEST_GW
    if args.env == "prod":
        gateway = PROD_GW

    username = getpass.getuser()

    SSH_EXTRA_OPTIONS=""
    if args.skip_host_key_check != None:
        SSH_EXTRA_OPTIONS = " ".join(["-o StrictHostKeyChecking=no",
                                      "-o UserKnownHostsFile=/dev/null",
                                      "-o GlobalKnownHostsFile=/dev/null"])

    dest_base = os.path.basename(dest_dir)
    dos2unix(dest_dir)
    scp_command = "scp -p {} -r {} {}@{}.prod.avvo.com:~".format(SSH_EXTRA_OPTIONS,
                                                                 dest_base, username,
                                                                 gateway)
    os.system(scp_command)

    log = "Executed command:\n{}\n\nDone.\n".format(scp_command)
    sys.stdout.write(log)

if __name__ == '__main__':

    # Setup vars
    deploy_file_path = os.path.realpath(__file__)
    common_dir = os.path.dirname(deploy_file_path)

    project_dir = os.getcwd()
    project_name = os.path.basename(project_dir)
    dest_dir = os.path.join(project_dir, 'deploy-' + project_name)

    # Program exec
    args = parse_args()
    create_dest_dir(dest_dir)
    write_properties(args, project_dir, common_dir, dest_dir)
    adjust_properties(args, dest_dir)
    copy_files(args, project_dir, common_dir, dest_dir)

    # Send deploy dir to gateway
    if not args.no_scp:
        run_scp(args, dest_dir)
