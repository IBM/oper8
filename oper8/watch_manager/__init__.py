"""
Top-level watch_manager imports
"""

# Local
from .ansible_watch_manager import AnsibleWatchManager
from .base import WatchManagerBase
from .dry_run_watch_manager import DryRunWatchManager
from .python_watch_manager import PythonWatchManager

# Expose the static start/stop functions at the top
start_all = WatchManagerBase.start_all
stop_all = WatchManagerBase.stop_all
