#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 13 07:11:27 2025

@author: imchugh
"""

#------------------------------------------------------------------------------
import argparse
from tasks import tasks_new as tasks
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def main():
    
    # Configure the parser
    parser = argparse.ArgumentParser(
        description="Run a task from the tasks module."
        )
    parser.add_argument(
        "task",
        help="The name of the task to run"
        )

    # Get the arguments
    args = parser.parse_args()

    # Run the task
    tasks.run_task(task=args.task)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
if __name__ == '__main__':
    
    main()
#------------------------------------------------------------------------------
