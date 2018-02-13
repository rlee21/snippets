import os
import shutil
import getpass
import sys
import subprocess
import tempfile

from common import WdepError, JOB_PROP, JOB_PROP_JSON
from package import package_name
from fabric.api import local, settings, abort, run, cd, sudo
from fabric.contrib.project import upload_project
from fabric.state import env

GW = { 'test': 'cg1test', 'prod': 'cg2wow' }

# @ToDo:
#    - virtualenv
#    - test error cases

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

def read_prop_vars(package):
    with open(os.path.join(package, JOB_PROP)) as jp:
        line = jp.readline().rstrip('\n')
        wdep_vars = line.split(":")

    if wdep_vars[0] == '#__WDEPVARS__':
        ns_env = wdep_vars[1].split(',')
        return ns_env[0], ns_env[1], ns_env[2]
    else:
        return None, None, None

def read_app_path(package):
    with open(os.path.join(package, JOB_PROP)) as jp:
        for line in jp:
            line = line.rstrip('\n')
            prop = line.split('=')
            if prop[0] == 'applicationPath':
                return prop[1]
    return None

def validate(app_path, name_space):
    if app_path == None:
        raise WdepError("Cannot read applicationPath, check your job.properties" \
                        "to make sure you have the applicationPath variable defined")

    deploy_location_error = "Error: You are deploying with a name_space of '{}' however your " \
                            "application path is '{}'. You should be deploying to /user/${{user.name}}/ for the " \
                            "'user' name_space or /shared/workflow/ for the 'final' name_space".format(name_space, app_path)
    if name_space == 'user' and not (app_path.startswith('hdfs:///user/${user.name}/') or \
                                     app_path.startswith('/user/${user.name}/')):
        raise WdepError(deploy_location_error)
    if name_space == 'final' and not (app_path.startswith('hdfs:///shared/workflow/') or \
                                      app_path.startswith('/shared/workflow/')):
        raise WdepError(deploy_location_error)

def deploy(args):

    user = getpass.getuser()
    package = args.package or package_name(os.getcwd())
    if package == None or not os.path.exists(package):
        raise WdepError("Wdep cannot find the package you are trying to deploy. Make" \
                        "sure the package exists and you are running wdep from the" \
                        "correct directory or specify the package using -p/--package" \
                        "PACKAGEPATH")

    # @Improve: add some log printing to this deploy function
    read_ns, read_env, read_venv = read_prop_vars(package) # @Improve: use args as primary and reads as secondary
    gateway = args.environment or read_env
    if gateway == None:
        raise WdepError("Cannot read deploy environment, either re-package or specify" \
                        "the environment using -e/--environment [test|prod]")
    app_path = read_app_path(package)
    validate(app_path, read_ns)

    app_path_formated = app_path.replace('${user.name}', user)

    env.user = user
    env.host_string = "{}.prod.avvo.com".format(GW[gateway])

    if sys.platform == 'win32':
        dos2unix(package)






    # Entering fabric code:

    upload_project(package)

    # Get full gateway path
    with cd(package):
        gw_path = run('pwd')

    # Create virtualenv if needed
    if args.virtualenv or read_venv == 'True': # @Refactor: put this in separate function
        with cd(gw_path):
            run('python2.7 {}/venv.py'.format(gw_path))

    #script_cmd = 'bash {}/hadoop_deploy.sh {}'.format(gw_path, app_path_formated)
    #if args.as_user:
    #    run(script_cmd)
    #else:
    #    run('echo {} | sudo su - jobrunner'.format(script_cmd))

