import os
import shutil
import sys
import argparse
import json
import config
import subprocess
import pip
from datetime import datetime
from common import JOB_PROP, JOB_PROP_JSON

# @ToDo: update package to create correct dates within job.properties and correct running freq

def add_properties(args, source_path, output_file):
    """ Read properties json and create oozie properties file """
    if os.path.exists(source_path):
        prop_file = open(source_path)
        prop_json = json.load(prop_file)

        for segment in prop_json:
            if (segment["environment"] == args.environment or segment["environment"] == "*") and \
               (segment["namespace"] == args.namespace or segment["namespace"] == "*"):
                output_file.write("# Environment: {}, Namespace: {}\n".format(segment["environment"],
                                                                              segment["namespace"]))
                properties = segment["properties"]
                for prop in properties:
                    output_file.write("{}={}\n".format(prop["name"], prop["value"]))
                output_file.write("\n")

def create_job_properties(args, package_path):

    output_job_path = os.path.join(package_path, JOB_PROP)
    common_job_path = os.path.join(config.__path__[0], JOB_PROP_JSON)
    project_job_path = os.path.join(args.path, JOB_PROP_JSON)

    output_file = open(output_job_path, "w")

    # Add namespace and environment vars to be read by the deploy process
    output_file.write("#__WDEPVARS__:{},{},{}\n\n".format(args.namespace,
                                                          args.environment, # @Check: is this needed?
                                                          args.virtualenv))

    output_file.write("# Common properties:\n\n")
    add_properties(args, common_job_path, output_file)

    output_file.write("# Project properties:\n\n")
    add_properties(args, project_job_path, output_file)

    output_file.close()

def package_name(base_path):
    dir_name = os.path.basename(base_path)
    return '-'.join(['deploy', dir_name])

def package(args):
    package_dir = package_name(args.path)
    exclude = set() if args.exclude == None else set(f for f in args.exclude.split(','))

    sys.stdout.write("\nCreating package directory: {}\n".format(package_dir))
    if os.path.exists(package_dir):
        shutil.rmtree(package_dir)
    os.mkdir(package_dir)

    root, dirs, files = next(os.walk(args.path))

    sys.stdout.write("\nCopying files:\n")
    create_job_properties(args, package_dir)
    for f in files:
        if f not in exclude:
            sys.stdout.write("  {}\n".format(f))
            shutil.copy(f, os.path.join(package_dir, f))

    sys.stdout.write("\nCopying directories:\n")
    for d in dirs:
        if d != package_dir and d not in exclude:
            sys.stdout.write("  {}\n".format(d))
            shutil.copytree(d, os.path.join(package_dir, d))

    sys.stdout.write("\nCopying common files:\n")
    scripts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scripts')
    root, dirs, files = next(os.walk(scripts_dir))
    for cf in files:
        sys.stdout.write("  {}\n".format(cf))
        shutil.copy(os.path.join(scripts_dir, cf), os.path.join(package_dir, cf))
    sys.stdout.write("\nDone\n")

