import os
import sys
import pip
import subprocess

# @Bugs: this does not work sigh

# @Improve: should probably be tar balling the venv
sys.stdout.write("\nCreating virtualenv:\n")
venv_dir = '.env'
virtualenv_call = subprocess.Popen(['virtualenv', venv_dir])
#virtualenv_call = subprocess.Popen(['virtualenv', venv_dir])
virtualenv_call.wait()

activate_this = os.path.join(venv_dir, 'bin', 'activate_this.py')
execfile(activate_this, dict(__file__=activate_this))

# @ErrorCheck: Perform some checking on the requirements file
sys.stdout.write("\nInstalling packages:\n")
pip.main(['install', '-I', '-U', '-r', 'requirements.txt'])
sys.stdout.write("\n")

