# Assumes Mamba has been installed under $HOME/mambaforge
eval "$($HOME/mambaforge/bin/conda shell.bash hook)"

mamba create -n root-env --file requirements_root.txt
