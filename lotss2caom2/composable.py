# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2023.                            (c) 2023.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

"""
Implements the default entry point functions for the workflow
application.

'run' executes based on either provided lists of work, or files on disk.
'run_incremental' executes incrementally, usually based on time-boxed intervals.

What I think the order of operations is (pagination is probably not involved):
1. Get the number of records - get the IDs for all the records?
    - 841 Mosaic IDs, how many files per mosaic?
        - 1 for P000+23
        - also 1 for P124+62
    - max record length == 1000? (confirm this) - from what I can tell, there is no max record length with the
      TAP service (100,000 rows from the form, anyway - https://vo.astron.nl/__system__/adql/query/form)
    - 1 Observation / Row from querying lotss_dr2.mosaics from https://vo.astron.nl/tap
2. For each Mosaic ID, get all the metadata from lotss_dr2.mosaics
    - query the lotss_dr2.mosaics table
    - then query the related_products page - this is where the links for the file headers are found
    - get the file headers for each mosaic and process each header
        - 1 Plane / Mosaic
        - n Artifacts / Plane - one for each link on the related_products page - or just 1 for each of the
        files in the header package?

The supported task type will be INGEST (no data will be transferred, so no SCRAPE/STORE/MODIFY).

A re-ingest will always hit ASTRON, as there's no data at CADC to scrape.
"""

import logging
import sys
import traceback

from caom2pipe.manage_composable import Config
from caom2pipe.name_builder_composable import EntryBuilder
from caom2pipe.run_composable import run_by_todo
from lotss2caom2 import clients, data_source, fits2caom2_augmentation, main_app, metadata_reader, preview_augmentation


META_VISITORS = [fits2caom2_augmentation, preview_augmentation]
DATA_VISITORS = []


def _common_init():
    builder = EntryBuilder(main_app.LOTSSName)
    config = Config()
    config.get_executors()
    astron_clients = clients.ASTRONClientCollection(config)
    reader = metadata_reader.LOTSSDR2MetadataReader(astron_clients, config.http_get_timeout)
    return builder, config, astron_clients, reader


def _run():
    """
    Uses a todo file to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    builder, config, astron_clients, reader = _common_init()
    return run_by_todo(
        clients=astron_clients,
        config=config,
        meta_visitors=META_VISITORS,
        data_visitors=DATA_VISITORS,
        name_builder=builder,
        metadata_reader=reader,
    )


def run():
    """Wraps _run in exception handling, with sys.exit calls."""
    try:
        result = _run()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)


def _run_remote():
    """
    Uses a todo file to identify the work to be done.

    :return 0 if successful, -1 if there's any sort of failure. Return status
        is used by airflow for task instance management and reporting.
    """
    builder, config, astron_clients, reader = _common_init()
    source = data_source.ASTRONPyVODataSource(config, astron_clients)
    return run_by_todo(
        clients=astron_clients,
        config=config,
        meta_visitors=META_VISITORS,
        data_visitors=DATA_VISITORS,
        name_builder=builder,
        sources=[source],
        metadata_reader=reader,
    )


def run_remote():
    """Wraps _run in exception handling, with sys.exit calls."""
    try:
        result = _run_remote()
        sys.exit(result)
    except Exception as e:
        logging.error(e)
        tb = traceback.format_exc()
        logging.debug(tb)
        sys.exit(-1)
