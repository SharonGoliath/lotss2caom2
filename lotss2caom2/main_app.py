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
        self._product_id = f'{self._mosaic_id}_mosaic'

    def get_artifact_product_type(self, value):
        result = ProductType.SCIENCE
        if '-rms.' in value:
            result = ProductType.NOISE
        elif '-blanked.' in value:
            result = ProductType.AUXILIARY
        elif '-weights.' in value:
            result = ProductType.WEIGHT
        elif '.jpg' in value:
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
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri):
        super().__init__(storage_name, headers, clients, observable, observation, config)
        self._mosaic_metadata = mosaic_metadata[0]
        self._dest_uri = dest_uri

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level.

        hard-coded values are from https://science.astron.nl/sdc/astron-data-explorer/data-releases/lotss-dr2/
        """
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
        bp.set('Observation.proposal.keywords', '_get_observation_proposal_keywords()')

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
        bp.set('Plane.provenance.project', 'LoTSS DR2')
        bp.set('Plane.provenance.producer', 'ASTRON')
        bp.set('Plane.provenance.runID', self._mosaic_metadata['data_pid'])
        bp.set('Plane.provenance.lastExecuted', '')
        bp.set('Plane.provenance.reference', self._mosaic_metadata['related_products'])

        product_type = ProductType.SCIENCE
        if 'weight' in self._dest_uri:
            product_type = ProductType.WEIGHT
        bp.set('Artifact.productType', product_type)
        bp.set('Artifact.releaseType', 'data')

        bp.configure_time_axis(5)
        bp.set('Chunk.time.axis.axis.ctype', 'TIME')
        bp.set('Chunk.time.axis.axis.cunit', 'd')
        bp.set('Chunk.time.axis.function.naxis', 1)
        bp.set('Chunk.time.axis.function.delta', 16 / 24.0)
        bp.set('Chunk.time.axis.function.refCoord.pix', 0.5)
        bp.set('Chunk.time.axis.function.refCoord.val', self._mosaic_metadata['dateobs'])
        bp.set('Chunk.time.resolution', 8)
        bp.set('Chunk.time.timesys', 'UTC')

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
                            # no cut-out support for the Temporal Axis
                            chunk.time_axis = None
                            if artifact.uri == self._dest_uri and chunk.naxis == 4 and chunk.polarization is None:
                                # mosaic.fits has only two axes
                                chunk.naxis = 2
                                chunk.energy_axis = None
                            if chunk.naxis == 4 and artifact.uri in [
                                f'astron:LOTSS/{self._storage_name.mosaic_id}/mosaic-rms.fits',
                                f'astron:LOTSS/{self._storage_name.mosaic_id}/mosaic.pybdsmmask.fits',
                                f'astron:LOTSS/{self._storage_name.mosaic_id}/mosaic.resid.fits',
                            ]:
                                # Spectral WCS values from file headers result in negative wavelengths
                                chunk.naxis = 3
        return self._observation

    def _get_observation_proposal_keywords(self, ext):
        # values from https://www.aanda.org/articles/aa/full_html/2022/03/aa42484-21/aa42484-21.html
        # default keyword setting splits on whitespace
        temp = set()
        temp.add('surveys')
        temp.add('catalogs')
        temp.add('radio continuum: general')
        temp.add('techniques: image processing')
        return temp

    def _get_provenance_keywords(self, ext):
        # from https://vo.astron.nl/__system__/dc_tables/show/tableinfo/lotss_dr2.mosaics
        d = {
            'C': 'original',
            'F': 'resampled',
            'Z': 'fluxes valid',
            'X': 'not resampled',
            'V': 'for display only',
        }
        temp = set()
        temp.add(d.get(self._mosaic_metadata['pixflags']))
        return temp

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


class DR2MosaicScience(DR2MosaicAuxiliaryMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri):
        super().__init__(storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        super().accumulate_blueprint(bp)
        bp.set('Plane.provenance.name', '_get_provenance_name()')
        bp.set('Plane.provenance.version', '_get_provenance_version()')
        bp.set('Plane.provenance.keywords', '_get_provenance_keywords()')

        bp.configure_position_axes((1, 2))
        bp.add_attribute('Chunk.position.axis.function.cd11', 'CDELT1')
        bp.set('Chunk.position.axis.function.cd12', 0.0)
        bp.set('Chunk.position.axis.function.cd21', 0.0)
        bp.add_attribute('Chunk.position.axis.function.cd22', 'CDELT2')

        self._logger.debug('Done accumulate_bp.')


class DR2Mosaic(DR2MosaicAuxiliaryMapping):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri):
        super().__init__(storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        super().accumulate_blueprint(bp)

        bp.set('Artifact.contentType', 'application/fits')
        bp.set('Artifact.contentLength', self._mosaic_metadata['accsize'].item())

        bp.configure_position_axes((1, 2))
        bp.set('Chunk.position.coordsys', self._mosaic_metadata['refframe'])
        bp.set('Chunk.position.equinox', self._mosaic_metadata['wcs_equinox'])
        # bp.set('Chunk.position.axis.axis1.ctype', self._mosaic_metadata['wcs_projection'])
        bp.set('Chunk.position.axis.axis1.ctype', 'RA---SIN')
        bp.set('Chunk.position.axis.axis1.cunit', 'deg')
        # bp.set('Chunk.position.axis.axis2.ctype', self._mosaic_metadata['wcs_projection'])
        bp.set('Chunk.position.axis.axis2.ctype', 'DEC--SIN')
        bp.set('Chunk.position.axis.axis2.cunit', 'deg')
        cd_matrix = self._mosaic_metadata['wcs_cdmatrix']
        bp.set('Chunk.position.axis.function.cd11', cd_matrix[0].item())
        bp.set('Chunk.position.axis.function.cd12', cd_matrix[1].item())
        bp.set('Chunk.position.axis.function.cd21', cd_matrix[2].item())
        bp.set('Chunk.position.axis.function.cd22', cd_matrix[3].item())
        pixel_size = self._mosaic_metadata['pixelsize']
        bp.set('Chunk.position.axis.function.dimension.naxis1', pixel_size[0].item())
        bp.set('Chunk.position.axis.function.dimension.naxis2', pixel_size[1].item())
        ref_pixel = self._mosaic_metadata['wcs_refpixel']
        ref_values = self._mosaic_metadata['wcs_refvalues']
        bp.set('Chunk.position.axis.function.refCoord.coord1.pix', ref_pixel[0].item())
        bp.set('Chunk.position.axis.function.refCoord.coord1.val', ref_values[0].item())
        bp.set('Chunk.position.axis.function.refCoord.coord2.pix', ref_pixel[1].item())
        bp.set('Chunk.position.axis.function.refCoord.coord2.val', ref_values[1].item())

        bp.configure_energy_axis(4)
        # TODO how to translate "2 channels per 0.195 MHz subband" to resolution?
        bp.set('Chunk.energy.specsys', 'TOPOCENT')
        bp.set('Chunk.energy.bandpassName', self._mosaic_metadata['bandpassid'])
        bp.set('Chunk.energy.axis.axis.ctype', 'WAVE')
        bp.set('Chunk.energy.axis.axis.cunit', self._mosaic_metadata['bandpassunit'])
        bp.set('Chunk.energy.axis.range.start.pix', 0.5)
        bp.set('Chunk.energy.axis.range.start.val', self._mosaic_metadata['bandpasshi'])
        bp.set('Chunk.energy.axis.range.end.pix', 1.5)
        bp.set('Chunk.energy.axis.range.end.val', self._mosaic_metadata['bandpasslo'])

        self._logger.debug('Done accumulate_bp.')


class DR2MosaicSciencePolarization(DR2MosaicScience):
    def __init__(self, storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri):
        super().__init__(storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri)

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        super().accumulate_blueprint(bp)
        bp.set('Plane.provenance.name', '_get_provenance_name()')
        bp.set('Plane.provenance.version', '_get_provenance_version()')
        bp.set('Plane.provenance.keywords', '_get_provenance_keywords()')

        # if I use the 4th axis values from the files mosaic.resid.fits, mosaic-rms.fits, and mosaic.pybdsmmask.fits
        # the Frequency ends up negative at the Plane level.
        #
        # Leave those values out for now
        #
        # bp.configure_energy_axis(4)
        # # # TODO how to translate "2 channels per 0.195 MHz subband" to resolution?
        # bp.set('Chunk.energy.bandpassName', self._mosaic_metadata['bandpassid'])

        bp.configure_polarization_axis(3)
        self._logger.debug('Done accumulate_bp.')


def mapping_factory(storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri):
    result = None
    if dest_uri == f'{storage_name.scheme}:{storage_name.collection}/{storage_name.mosaic_id}/mosaic.fits':
        result = DR2Mosaic(storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri)
    else:
        if headers[0].get('NAXIS') == 2:
            result = DR2MosaicScience(
                storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri
            )
        else:
            result = DR2MosaicSciencePolarization(
                storage_name, headers, clients, observable, observation, config, mosaic_metadata, dest_uri
            )
    logging.debug(f'Created {result.__class__.__name__} for {dest_uri}')
    return result
