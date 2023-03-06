# Assumes Mamba has been installed under $HOME/mambaforge
eval "$($HOME/mambaforge/bin/conda shell.bash hook)"

mamba create -n coffea-env python==3.9.16

conda activate coffea-env

python -m pip install -r requirements_coffea.txt
