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
This module implements the ObsBlueprint mapping, as well as the workflow
entry point that executes the workflow.
"""

import logging

from os.path import basename, dirname

from caom2 import ProductType
from caom2pipe.astro_composable import get_geocentric_location
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc


__all__ = ['LOTSSName', 'mapping_factory']


class LOTSSName(mc.StorageName):
    """
    The unit of work for a StorageName is the Observation ID. The file name are all found in the
    MetadataReader specialization based on that Observation ID.

    The destination URIs are set in the MetadataReader, and the file_uri is set to make preview generation work.

    Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage

    observationID example: P124+62
    """

    LOTSS_NAME_PATTERN = '*'

    def __init__(self, entry):
        self._mosaic_id = basename(entry)
        super().__init__(
             obs_id=f'{self._mosaic_id}_dr2',
             file_name='',
             source_names=[entry],
        )
        self._product_id = 'mosaic'

    @property
    def artifact_product_type(self):
        result = ProductType.SCIENCE
        if '.rms.' in self.file_name:
            result = ProductType.NOISE
        elif '-blanked.' in self.file_name:
            result = ProductType.AUXILIARY
        elif '-weights.' in self.file_name:
            result = ProductType.WEIGHT
        elif self.file_name.endswith('.jpg'):
            result = ProductType.PREVIEW
        return result

    @property
    def file_uri(self):
        # make preview generation work
        return f'{mc.StorageName.scheme}:{mc.StorageName.collection}/preview.jpg'

    @property
    def mosaic_id(self):
        return self._mosaic_id

    def set_destination_uris(self):
        # fake a single entry so that the MetadataReader specialization does something
        self._destination_uris.append(f'{mc.StorageName.collection}/{self._mosaic_id}/')

    def set_file_id(self):
        pass


class DR2MosaicAuxiliaryMapping(cc.TelescopeMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata):
        super().__init__(storage_name, headers, clients, observable, observation, config)
        self._mosaic_metadata = mosaic_metadata

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)

        release_date = '2023-01-01T00:00:00.000'
        bp.set('Observation.metaRelease', release_date)
        bp.set('Observation.type', 'OBJECT')
        bp.set('DerivedObservation.members', [])

        bp.set('Observation.algorithm.name', 'mosaic')

        bp.set('Observation.instrument.name', self._mosaic_metadata['instid'])

        bp.set('Observation.proposal.id', 'LoTSS')
        bp.set('Observation.proposal.pi', 'T.W. Shimwell')
        bp.set('Observation.proposal.title', 'LOFAR Two-metre Sky Survey')
        # from https://www.aanda.org/articles/aa/full_html/2022/03/aa42484-21/aa42484-21.html
        bp.set('Observation.proposal.keywords', 'surveys,catalogs,radio continuum: general,techniques: image processing')

        bp.set('Observation.target.type', 'field')

        telescope_name = 'LOFAR'
        bp.set('Observation.telescope.name', telescope_name)
        x, y, z = get_geocentric_location(telescope_name)
        bp.set('Observation.telescope.geoLocationX', x)
        bp.set('Observation.telescope.geoLocationY', y)
        bp.set('Observation.telescope.geoLocationZ', z)

        bp.set('Plane.metaRelease', release_date)
        bp.set('Plane.dataRelease', release_date)
        bp.set('Plane.calibrationLevel', 4)
        bp.set('Plane.dataProductType', 'image')
        bp.set('Plane.provenance.name', '_get_provenance_name()')
        bp.set('Plane.provenance.version', '_get_provenance_version()')
        bp.set('Plane.provenance.project', 'LoTSS DR2')
        bp.set('Plane.provenance.producer', 'ASTRON')
        bp.set('Plane.provenance.runID', self._mosaic_metadata['data_pid'])
        bp.set('Plane.provenance.lastExecuted', '')
        bp.set('Plane.provenance.reference', self._mosaic_metadata['related_products'])
        bp.set('Plane.provenance.keywords', '_get_provenance_keywords()')

        bp.set('Artifact.productType', self._storage_name.artifact_product_type)
        bp.set('Artifact.releaseType', 'data')

        self._logger.debug('Done accumulate_bp.')

    def update(self, file_info):
        """Called to fill multiple CAOM model elements and/or attributes (an n:n relationship between TDM attributes
        and CAOM attributes).
        """
        super().update(file_info)
        for plane in self._observation.planes.values():
            if len(plane.artifacts) >= len(self._storage_name.destination_uris):
                for artifact in plane.artifacts.values():
                    for part in artifact.parts.values():
                        for chunk in part.chunks:
                            # no cut-out support
                            chunk.time_axis = None
                self._add_mosaic_artifact(plane)
        return self._observation

    def _add_mosaic_artifact(self, plane):
        """Add an artifact for the mosaic image itself."""
        self._logger.debug(f'Begin _add_mosaic_artifact for {plane.product_id}')
        artifact_uri = dirname(self._storage_name.destination_uris[0])
        orig_artifact = plane.artifacts[f'{artifact_uri}/mosaic-weights.fits']
        if artifact_uri in plane.artifacts.keys():
            plane.artifacts.pop(artifact_uri)
        artifact = cc.copy_artifact(orig_artifact)
        artifact.content_type = 'application/fits'
        artifact.content_length = self._mosaic_metadata['accsize'].item()
        artifact.uri = artifact_uri
        pixel_size = self._mosaic_metadata['pixelsize']
        ref_pixel = self._mosaic_metadata['wcs_refpixel']
        cd_matrix = self._mosaic_metadata['wcs_cdmatrix']
        for orig_part in orig_artifact.parts.values():
            part = cc.copy_part(orig_part)
            artifact.parts.add(part)
            for orig_chunk in orig_part.chunks:
                chunk = cc.copy_chunk(orig_chunk)
                if chunk.position is not None and chunk.position.axis is not None and chunk.position.axis.function is not None:
                    chunk.position.axis.function.cd11 = cd_matrix[0].item()
                    chunk.position.axis.function.cd12 = cd_matrix[1].item()
                    chunk.position.axis.function.cd21 = cd_matrix[2].item()
                    chunk.position.axis.function.cd22 = cd_matrix[3].item()
                    chunk.position.axis.function.dimension.naxis1 = pixel_size[0].item()
                    chunk.position.axis.function.dimension.naxis2 = pixel_size[1].item()
                    chunk.position.axis.function.ref_coord.coord1.pix = ref_pixel[0].item()
                    chunk.position.axis.function.ref_coord.coord2.pix = ref_pixel[1].item()
                part.chunks.append(chunk)
        plane.artifacts[artifact_uri] = artifact
        self._logger.debug('End _add_mosaic_artifact')

    def _get_provenance_keywords(self, ext):

        d = {
            'C': 'original',
            'F': 'resampled',
            'Z': 'fluxes valid',
            'X': 'not resampled',
            'V': 'for display only',
        }
        temp = self._mosaic_metadata['pixflags']
        return d.get(temp)

    def _get_provenance_name(self, ext):
        temp = self._headers[0].get('ORIGIN')
        result = 'ddf-pipeline'
        if temp:
            result = temp.split()[0]
        return result

    def _get_provenance_version(self, ext):
        temp = self._headers[0].get('ORIGIN')
        result = None
        if temp:
            result = temp.split()[1]
        return result

    def _update_artifact(self, artifact):
        # TODO - clean up unnecessary execution
        # self._logger.error(f'working on {artifact.uri}')
        pass

    def _update_plane(self, plane):
        # TODO - clean up unnecessary execution
        # self._logger.error(f'working on plane {plane.product_id}')
        pass


class DR2MosaicScienceMapping(DR2MosaicAuxiliaryMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata):
        super().__init__(storage_name, headers, clients, observable, observation, config, mosaic_metadata)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        super().accumulate_blueprint(bp)
        # hard-coded values are from https://science.astron.nl/sdc/astron-data-explorer/data-releases/lotss-dr2/

        bp.configure_position_axes((1, 2))
        bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        bp.configure_energy_axis(4)
        # TODO how to translate "2 channels per 0.195 MHz subband" to resolution?

        bp.configure_polarization_axis(3)

        bp.configure_time_axis(5)
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.function.naxis', 1)
        bp.set('Chunk.time.axis.function.delta', 16 / 24.0)
        bp.set('Chunk.time.axis.function.refCoord.pix', 0.5)
        bp.set('Chunk.time.axis.function.refCoord.val', self._mosaic_metadata['dateobs'])
        bp.set('Chunk.time.resolution', 8)
        self._logger.debug('Done accumulate_bp.')


class DR2MosaicSciencePolarization(DR2MosaicScienceMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata):
        super().__init__(storage_name, headers, clients, observable, observation, config, mosaic_metadata)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        super().accumulate_blueprint(bp)
        # hard-coded values are from https://science.astron.nl/sdc/astron-data-explorer/data-releases/lotss-dr2/

        bp.configure_energy_axis(4)
        # TODO how to translate "2 channels per 0.195 MHz subband" to resolution?

        bp.configure_polarization_axis(3)
        self._logger.debug('Done accumulate_bp.')


def mapping_factory(storage_name, headers, clients, observable, observation, config, mosaic_metadata):
    result = None
    if storage_name.artifact_product_type in [ProductType.AUXILIARY, ProductType.WEIGHT, ProductType.PREVIEW]:
        result = DR2MosaicAuxiliaryMapping(
            storage_name, headers, clients, observable, observation, config, mosaic_metadata
        )
    else:
        if headers[0].get('NAXIS') == 2:
            result = DR2MosaicScienceMapping(
                storage_name, headers, clients, observable, observation, config, mosaic_metadata
            )
        else:
            result = DR2MosaicSciencePolarization(
                storage_name, headers, clients, observable, observation, config, mosaic_metadata
            )
    logging.debug(f'Created {result.__class__.__name__} for {storage_name.file_name}')
    return result
