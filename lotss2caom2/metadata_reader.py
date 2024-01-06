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

import tarfile
from os import listdir, unlink
from os.path import basename, dirname, exists
from caom2pipe.astro_composable import get_vo_table_session, make_headers_from_file
from caom2pipe.manage_composable import http_get
from caom2pipe.reader_composable import MetadataReader


__all__ = ['LOTSSDR2MetadataReader']


class LOTSSDR2MetadataReader(MetadataReader):
    def __init__(self, clients, http_get_timeout):
        super().__init__()
        self._service = clients.py_vo_tap_client
        self._session = clients.https_session
        self._mosaic_id = None
        self._mosaic_uri = None
        # the page that lists all the files for a MOSAIC
        self._related_products_uri = None
        # the Preview URL
        self._preview_uri = None
        # the URL that links to the tar of the FITS headers from the related_products URI
        self._headers_uri = None
        self._destination_uris = []
        self._http_get_timeout = http_get_timeout

    def _retrieve_file_info(self, key, source_name):
        """
        There are no files at CADC except for the preview and thumbnail, so this information is not necessary.
        """
        pass

    def _retrieve_headers(self, key, source_name):
        """
        :param key: Artifact URI
        :param source_name: fully-qualified name at the data source
        """
        mosaic_id = basename(source_name)
        self._get_mosaic_tap_metadata(mosaic_id)
        self._get_related_products_metadata()
        self._get_headers_metadata()

    def _get_mosaic_tap_metadata(self, mosaic_id):
        """Get all the metadata from the TAP query for a mosaic id

        Field names at time of writing:
        ('accref', 'owner', 'embargo', 'mime', 'accsize', 'centeralpha', 'centerdelta', 'imagetitle', 'instid',
         'dateobs', 'naxes', 'pixelsize', 'pixelscale', 'refframe', 'wcs_equinox', 'wcs_projection', 'wcs_refpixel',
         'wcs_refvalues', 'wcs_cdmatrix', 'bandpassid', 'bandpassunit', 'bandpassrefval', 'bandpasshi',
         'bandpasslo', 'pixflags', 'coverage', 'mosaic_id', 'related_products', 'lofar_obsids', 'data_pid',
         'adler32')
        """
        self._logger.debug(f'Begin _get_mosaic_tap_metadata for {mosaic_id}')
        results = self._service.search(f"SELECT * FROM lotss_dr2.mosaics WHERE mosaic_id='{mosaic_id}'")
        if len(results) == 1:
            self._mosaic_id = mosaic_id
            self._mosaic_uri = results[0]['accref']
            self._headers[self._mosaic_uri] = [results[0]]
            self._related_products_uri = self._headers[self._mosaic_uri][0]['related_products']
            self._logger.debug(f'Found mosaic metadata for {mosaic_id}')
        else:
            self._logger.warning(f'Wrong number of results {len(results)} when querying for mosaic {mosaic_id}.')
        self._logger.debug(f'End _get_mosaic_tap_metadata')

    def _get_related_products_metadata(self):
        """Go to the 'related_products' page and get all the metadata from that page."""
        self._logger.debug(f'Begin _get_related_products_metadata from {self._related_products_uri}')
        if self._related_products_uri:
            vo_table, error_message = get_vo_table_session(self._related_products_uri, self._session)
            for row in vo_table.array:
                access_url = row['access_url']
                if 'preview' in access_url:
                    self._preview_uri = access_url
                    self._logger.debug(f'Found preview URL {access_url}')
                elif 'fits' in access_url:
                    if 'headers.tar' in access_url:
                        self._headers_uri = access_url
                        self._logger.debug(f'Found Headers URL {access_url}')
                    else:
                        self._destination_uris.append(access_url)
                        self._logger.debug(f'Found FITS URL {access_url}')

            # Accomodate the difference between the 6 files in the VOTable listing, and the 7 fits headers in the
            # tar file.
            # "mosaic-blanked.fits.0.hdr" is the extra.
            if len(self._destination_uris) == 6:
                for entry in self._destination_uris:
                    if '/mosaic-weights' in entry:
                        temp = entry.replace('-weights', '-blanked')
                        self._destination_uris.append(temp)
                        self._logger.debug(f'Append {temp} to destination URIs')
        self._logger.debug(f'End _get_related_products_metadata')

    def _get_headers_metadata(self):
        """Retrieve the file that has all the header metadata in it, and get all the metadata from that file,
        for each of files."""
        self._logger.debug(f'Begin _get_headers_metadata for {self._headers_uri}')
        if self._headers_uri:
            local_fqn = f'/tmp/{basename(self._headers_uri)}'
            if exists(local_fqn):
                unlink(local_fqn)
            http_get(self._headers_uri, local_fqn, self._http_get_timeout)
            if exists(local_fqn):
                with tarfile.open(local_fqn) as f:
                    f.extractall('/tmp', filter='data')
                for entry in listdir('/tmp/fits_headers'):
                    found_uri = None
                    for uri in self._destination_uris:
                        if entry == 'mosaic.rms.fits.0.hdr':
                            found_uri = f'{dirname(uri)}/mosaic-rms.fits'
                            break
                        elif uri.endswith(f'/{entry.replace(".0.hdr", "")}'):
                            found_uri = uri
                            break
                    if found_uri:
                        self._headers[found_uri] = make_headers_from_file(f'/tmp/fits_headers/{entry}')
                        self._logger.debug(f'Retrieve headers for {found_uri}')
                    else:
                        self._logger.warning(f'Unexpected file header {entry}')
        self._logger.debug('End _get_headers_metadata')

    def _reset(self):
        self._mosaic_id = None
        self._related_products_uri = None
        self._preview_uri = None
        self._headers_uri = None
        self._destination_uris = []

    def reset(self):
        super().reset()
        self._reset()

    def set(self, storage_name):
        super().set(storage_name)
        self._logger.info(
            f'Replace destination URIs for {storage_name._destination_uris[0]} with {len(self._destination_uris)} '
            'new URIs'
        )
        # JJK/PD 05-01-24
        # Artifact download for https URLs does not work from sc2, so use "correct" metadata instead
        # Use URI pattern like: astron:LOTSS/p002+18/mosaic.fits
        uri_prefix = f'{storage_name.scheme}:{storage_name.collection}/{self._mosaic_id}/'
        storage_name._destination_uris = []
        for entry in self._destination_uris:
            new_uri = f'{uri_prefix}{basename(entry)}'
            storage_name._destination_uris.append(new_uri)
            self._headers[new_uri] = self._headers[entry]
            self._headers.pop(entry)
        storage_name._destination_uris.append(f'{uri_prefix}mosaic.fits')

    def unset(self, storage_name):
        super().unset(storage_name)
        self._reset()
