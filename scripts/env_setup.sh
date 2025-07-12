#!/usr/bin/env bash
set -e
export PATH="$HOME/.local/bin:$PATH"

if [ -d "$HOME/miniconda3/bin" ]; then
    export PATH="$HOME/miniconda3/bin:$PATH"
fi
if [ -d "$HOME/anaconda3/bin" ]; then
    export PATH="$HOME/anaconda3/bin:$PATH"
fi

ENV_NAME="wx-info-env"
PYTHON_VERSION="3.13"

# Function to check and install Poetry if not present
ensure_poetry() {
    if ! command -v poetry &> /dev/null; then
        echo "Poetry not found. Installing..."
        pip install --user poetry
        export PATH="$HOME/.local/bin:$PATH"
    fi
}

if command -v conda &> /dev/null; then
    echo "Conda detected."
    if ! conda info --envs | grep -q "^$ENV_NAME[[:space:]]"; then
        echo "Creating environment $ENV_NAME..."
        conda create -y -n $ENV_NAME python=$PYTHON_VERSION
    else
        echo "Conda environment $ENV_NAME already exists."
    fi
    echo "Activating environment..."
    source $(conda info --base)/etc/profile.d/conda.sh
    conda activate $ENV_NAME
    ensure_poetry
elif command -v miniconda &> /dev/null; then
    echo "Miniconda detected."
    if ! miniconda info --envs | grep -q "^$ENV_NAME[[:space:]]"; then
        echo "Creating environment $ENV_NAME..."
        miniconda create -y -n $ENV_NAME python=$PYTHON_VERSION
    else
        echo "Miniconda environment $ENV_NAME already exists."
    fi
    source $(miniconda info --base)/etc/profile.d/conda.sh
    conda activate $ENV_NAME
    ensure_poetry
else
    echo "Conda/Miniconda not found. Falling back to pip/virtualenv."
    if ! command -v python$PYTHON_VERSION &> /dev/null; then
        echo "Python $PYTHON_VERSION not found. Please install it."
        exit 1
    fi
    if [ ! -d "$ENV_NAME" ]; then
        echo "Creating venv $ENV_NAME..."
        python$PYTHON_VERSION -m venv $ENV_NAME
    else
        echo "Virtualenv $ENV_NAME already exists."
    fi
    source $ENV_NAME/bin/activate
    ensure_poetry
fi

echo "Installing dependencies with Poetry..."
poetry install 