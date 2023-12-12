
from distutils.command.config import config
from collections import UserDict
from ruamel import yaml
from ruamel.yaml.scanner import ScannerError

import os
import sys
import logging

log = logging.getLogger(__name__)

class Config(UserDict):


    def __init__(self, config_path = './config/conf.yaml'):
        self.config_path = os.path.expanduser(config_path)
        self.load()

    def load(self):
        """
        loads configuration set in the yaml file
        """
        try:
            with open(os.path.expanduser(self.config_path), 'r') as f:
                try:
                    self.data = yaml.safe_load(f)
                except ScannerError as e:
                    log.error(
                        'Error parsing YAML config file '
                        '{}:{}'.format(
                            e.problem_mark,
                            e.problem
                        )
                    )
                    sys.exit(1)
        except FileNotFoundError:
            log.error(
                'YAML file not found in {}'.format(
                    self.config_path
                )
            )
            sys.exit(1)


