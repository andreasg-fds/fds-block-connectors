# Formation Data Systems Block Based Connectors
This repository is home to Block based connectors for the FormationOne data
platform. The connectors utilize the eXtensible Data Interface which is
located here: (https://github.com/fds-dev/xdi-contracts)

The blocks connectors are loaded via a simple DLOPEN(3) interface by the
FormationOne AccessMgr.

Connectors currently exist for the following BLOCK protocols:
 * Network Block Device (https://github.com/NetworkBlockDevice/nbd)
 * Generic SCSI Target Linux Subsystem (https://github.com/bvanassche/scst)
