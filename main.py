"""Website monitoring Application

This programm allows one to fetch statistics from configured websites 
such as response time, website status, option to check for specific 
strings among the website contents, and error reporting.

This script requires that you to install a few tools before use. One 
is advised to refer `requirements.txt` file provided along with this 
project. Better option will be to run following command:
`pip install -r requirements.txt`

To run this program:
`python main.py`
"""

from src.utils.config_read import ConfigReader
from src.runner import Runner

if __name__ == "__main__":
    runner = Runner()
    procs = (runner.web_monitor_proc, runner.stats_consumer_proc)

    config = ConfigReader('config/config.json')

    runner.run_procs(procs, config)
