#! /usr/bin/env python3
import logging
import os
import subprocess
import sys

from pathlib import Path


from ops.framework import StoredState

from ops.charm import CharmBase

from ops.main import main

from ops.model import ActiveStatus

from adapters.framework import FrameworkAdapter


from slurm_ops_manager import SlurmOpsManager

from interface_slurmdbd import SlurmdbdProvidesRelation


class SlurmdbdCharm(CharmBase):
    _state = StoredState() 

    def __init__(self, *args):
        super().__init__(*args)

        self._state.set_default(db_info=None)
        
        self.slurm_ops_manager = SlurmOpsManager(self, "slurmdbd")
        self.slurmdbd = SlurmdbdProvidesRelation(self, "slurmdbd")

        self.db = MySQLClient(self, "db")

        self.fw_adapter = FrameworkAdapter(self.framework) 

        event_handler_bindings = {
            self.db.on.database_available: self._on_database_available,
            self.on.install: self._on_install,
            self.on.start: self._on_start,
        }
        for event, handler in event_handler_bindings.items():
            self.fw_adapter.observe(event, handler)
        
    def _on_install(self, event):
        handle_install(
            event,
            self.fw_adapter,
            self.slurm_ops_manager,
            self.slurmdbd,
        )

    def _on_start(self, event):
        self.unit.status = ActiveStatus("Slurmdbdb Available")

    def _on_database_available(self, event):
        handle_database_available(
            event,
            self.fw_adapter,
            self._state,
        )

def handle_install(event, fw_adapter, slurm_ops_manager, slurmdbd):
    """
    installs the slurm snap from edge channel if not provided as a resource
    then connects to the network
    """
    slurm_ops_manage.prepare_system_for_slurm()
    fw_adapter.set_unit_status(ActiveStatus("Slurm Installed"))


def handle_database_available(event, fw_adapter, state):
    """Render the database details into the slurmdbd.yaml and
    set the snap.mode.
    """

    state.db_info = {
        'user': event.db_info.user,
        'password': event.db_info.password,
        'host': event.db_info.host,
        'port': event.db_info.port,
        'database': event.db_info.database,
    }
    fw_adapter.set_unit_status(ActiveStatus("mysql available"))


if __name__ == "__main__":
    main(SlurmdbdCharm)
