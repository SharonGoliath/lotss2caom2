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
#  Revision: 4
#
# ***********************************************************************
#

import logging
import os
import shutil

from caom2 import ReleaseType, ProductType
from caom2pipe.manage_composable import CadcException, http_get, PreviewVisitor, StorageName


class LOTSSPreview(PreviewVisitor):
    """
    Generate a small thumbnail from a previously existing preview image. Override most of the existing
    class, because the original science file is unnecessary.
    """

    def __init__(self, **kwargs):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._release_type = ReleaseType.META
        self._mime_type = 'image/jpeg'
        self._working_dir = kwargs.get('working_directory', './')
        self._clients = kwargs.get('clients')
        if self._clients is None or self._clients.data_client is None:
            self._logger.warning('Visitor needs a clients.data_client parameter to store previews.')
        self._storage_name = kwargs.get('storage_name')
        if self._storage_name is None:
            raise CadcException('Visitor needs a storage_name parameter.')
        self._metadata_reader = kwargs.get('metadata_reader')
        self._preview_fqn = os.path.join(self._working_dir, self._storage_name.prev)
        self._thumb_fqn = os.path.join(self._working_dir, self._storage_name.thumb)
        self._delete_list = []
        # keys are uris, values are lists, where the 0th entry is a file name,
        # and the 1th entry is the artifact type
        self._previews = {}
        self._report = None
        self._hdu_list = None
        self._ext = None
        self._storage_name._file_name = 'preview.jpg'
        self._input_fqn = self._storage_name.get_file_fqn(self._working_dir)
        self._preview_fqn = os.path.join(os.path.dirname(self._input_fqn), self._storage_name.prev)
        self._thumb_fqn = os.path.join(os.path.dirname(self._input_fqn), self._storage_name.thumb)
        self._logger.debug(self)

    def visit(self, observation):
        count = 0
        if self._storage_name.product_id in observation.planes.keys():
            plane = observation.planes[self._storage_name.product_id]
            self._logger.debug(
                f'Preview generation for observation {observation.observation_id}, plane {plane.product_id}.'
            )
            count += self._do_prev(plane, observation.observation_id)
            self._augment_artifacts(plane)
            self._delete_list_of_files()
        self._logger.info(
            f'Completed preview augmentation for {observation.observation_id}. Changed {count} artifacts.'
        )
        self._report = {'artifacts': count}
        return observation

    def generate_plots(self, obs_id):
        count = 0
        self._logger.debug(f'Begin generate_plots for {obs_id} from {self._metadata_reader._preview_uri}')
        if self._metadata_reader._preview_uri:
            http_get(self._metadata_reader._preview_uri, self._input_fqn)
            if os.path.exists(self._input_fqn):
                self._logger.info(f'Retrieved {self._input_fqn}')
                shutil.copy(self._input_fqn, self._preview_fqn)
                self.add_preview(self._storage_name.prev_uri, self._storage_name.prev, ProductType.PREVIEW)
                count += self._gen_thumbnail()
                if count == 1:
                    self.add_preview(self._storage_name.thumb_uri, self._storage_name.thumb, ProductType.THUMBNAIL)
        self._logger.debug(f'End generate_plots')
        return count


def visit(observation, **kwargs):
    previewer = LOTSSPreview(**kwargs)
    return previewer.visit(observation)
