#! /usr/bin/env python3
import json
import logging
import os
import subprocess
import sys

from pathlib import Path


from ops.framework import StoredState

from ops.charm import CharmBase

from ops.main import main

from ops.model import (
    ActiveStatus,
    BlockedStatus,
)


from slurm_ops_manager import SlurmOpsManager

from interface_slurmdbd import SlurmdbdProvidesRelation

from interface_mysql import MySQLClient


class SlurmdbdCharm(CharmBase):
    _state = StoredState() 

    def __init__(self, *args):
        super().__init__(*args)

        self._state.set_default(db_info=None)
        self._state.set_default(db_info_acquired=False)
        
        self.slurm_ops_manager = SlurmOpsManager(self, "slurmdbd")
        self.slurmdbd = SlurmdbdProvidesRelation(self, "slurmdbd")

        self.db = MySQLClient(self, "db")

        event_handler_bindings = {
            self.db.on.database_available: self._on_database_available,
            self.on.install: self._on_install,
            self.on.start: self._on_start,
            self.on.config_changed: self._on_config_changed,
 
        }
        for event, handler in event_handler_bindings.items():
            self.framework.observe(event, handler)
        
    def _on_install(self, event):
        self.slurm_ops_manager.prepare_system_for_slurm()
        self.unit.status = ActiveStatus("Slurm Installed")

    def _on_start(self, event):
        pass
            
    def _on_config_changed(self, event):
        if not (self._state.db_info_acquired and self.slurm_ops_manager.slurm_installed):
            self.unit.status = BlockedStatus("Need relation to MySQL before starting slurmdbd.")
            event.defer()
            return
        write_config_and_restart_slurmdbd(self)

    def _on_database_available(self, event):
        """Render the database details into the slurmdbd.yaml and
        set the snap.mode.
        """

        self._state.db_info = {
            'db_username': event.db_info.user,
            'db_password': event.db_info.password,
            'db_hostname': event.db_info.host,
            'db_port': event.db_info.port,
            'db_name': event.db_info.database,
        }

        write_config_and_restart_slurmdbd(self)
        self._state.db_info_acquired = True
        

def write_config_and_restart_slurmdbd(charm): 
    slurmdbd_host_port_addr = {
        'slurmdbd_hostname': charm.slurm_ops_manager.hostname,
        'slurmdbd_port': charm.slurm_ops_manager.port,
    }
    slurmdbd_config = {
        **slurmdbd_host_port_addr,
        **charm.model.config,
        **charm._state.db_info,
    }
    charm.slurm_ops_manager.render_config_and_restart(slurmdbd_config)
    charm.unit.status = ActiveStatus("Slurmdbd Available")


if __name__ == "__main__":
    main(SlurmdbdCharm)
