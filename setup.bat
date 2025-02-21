@echo off
echo Setting up Python environment...
python -m venv envs\python_env
call envs\python_env\Scripts\activate
pip install -r requirements.txt
call envs\python_env\Scripts\deactivate

REM Set up R virtual environment
cd /d D:\Desktop\financial_data_pipeline\envs\r_env

REM Install R dependencies
Rscript -e "install.packages('renv')"
Rscript -e "renv::restore(lockfile = 'D:/Desktop/financial_data_pipeline/envs/r_env/renv.lock')"

echo Environment setup complete!
