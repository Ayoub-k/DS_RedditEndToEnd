#!/bin/bash

# Extract parameters
PROJECT_PATH=$1
NAME_ENV=$2
PYTHON_SCRIPT_PATH=$3

cd $PROJECT_PATH
export PROJECT_PATH=$PROJECT_PATH
export PYTHONPATH=$PROJECT_PATH
source $NAME_ENV/bin/activate

# Run the Python script
python $PYTHON_SCRIPT_PATH
