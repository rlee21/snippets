import os
import shutil
import sys
import argparse
import json
from datetime import datetime
from package import package
from deploy import deploy
from run import run

def parse_args():
    """ Parse command line arguments """

    cwd = os.getcwd()
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest = 'action')

    package_parser = subparsers.add_parser('package', help='Package workflow code and dependancies')
    package_parser.add_argument('-e', '--environment',
                        help="specify the environment (i.e. cluster) to run in, \
                        e.g. test, prod. (required field)",
                        required=True, choices=['test', 'prod'])
    package_parser.add_argument('-n', '--namespace',
                        help="namespace to run in. Reserved keys: user, final. \
                        (required field)", required=True)
    package_parser.add_argument('-p', '--path', default = cwd,
                                help='The path to your project, defaults to the current working directory')
    package_parser.add_argument('-x', '--exclude', default = None,
                                help='Comma separated list of files or dirs that should be excluded from the package')
    package_parser.add_argument('-v', '--virtualenv', default = False, action = 'store_true',
                                help='Create a python virtualenv on the gateway machine to be \
                                      used when running the workflow. This option requires a \
                                      requierments.txt file to exist in your package directory.')
    # @Implement: These options do not fully functional
    package_parser.add_argument('--start', default = datetime.now().strftime('%Y%m%dT%H:%MZ'))
    package_parser.add_argument('--end', default = '20250901T01:00Z')
    package_parser.add_argument('-i', '--include', default = None,
                                help='Comma separated list of files or dirs that should be included in the package')

    deploy_parser = subparsers.add_parser('deploy', help='Deploy code to the gateway machine')
    deploy_parser.add_argument('-e', '--environment', help="specify the environment (i.e. cluster) to run in, \
                                e.g. test, prod. (required field)", default = None, choices=['test', 'prod'])
    deploy_parser.add_argument('-v', '--virtualenv', default = False, action = 'store_true',
                               help='Create a python virtualenv on the gateway machine to be \
                                     used when running the workflow. This option requires a \
                                     requierments.txt file to exist in your package directory.')
    deploy_parser.add_argument('-p', '--path', default = cwd,
                               help='The path to your project, defaults to the current working directory')
    deploy_parser.add_argument('-P', '--package', default = None,
                               help='Path to your package, if not specified wdep will look in your path for the package')
    deploy_parser.add_argument('-u', '--as-user', action = 'store_true', default = False,
                               help='Run the deploy process as your user, NOT as jobrunner. Default is to run as jobrunner.')

    run_parser = subparsers.add_parser('run', help='Run workflow in the cluster')
    run_parser.add_argument('-n', '--namespace', choices = ['user', 'final'])
    run_parser.add_argument('-e', '--environment', choices = ['test', 'prod'])

    return parser.parse_args()

def main():
    func = { 'package': package,
             'deploy': deploy,
             'run': run }
    args = parse_args()
    func[args.action](args)

if __name__ == '__main__':
    main()

