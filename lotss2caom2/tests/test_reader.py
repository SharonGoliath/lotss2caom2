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

import shutil
from lotss2caom2.main_app import LOTSSName
from lotss2caom2.metadata_reader import LOTSSDR2MetadataReader
from mock import patch
import helpers


@patch('lotss2caom2.metadata_reader.http_get')
@patch('lotss2caom2.clients.ASTRONClientCollection')
def test_reader(clients_mock, http_get_mock, test_data_dir, test_config):
    clients_mock.py_vo_tap_client.search.side_effect = helpers._search_mosaic_id_mock

    def _endpoint_mock(url):
        assert (
            url == 'https://vo.astron.nl/lotss_dr2/q/dlmosaic/dlmeta?ID=ivo%3A//astron.nl/%7E%3FLoTSS-DR2/P000%2B23'
        ), f'wrong url {url}'
        result = type('response', (), {})()
        result.close = lambda: None
        with open(f'{test_data_dir}/P000+23/obs.xml') as f:
            result.text = f.read()
        return result

    clients_mock.https_session.get.side_effect = _endpoint_mock

    def _http_get_mock(url, fqn, ignore_timeout):
        assert url == 'https://lofar-webdav.grid.surfsara.nl:2881/P000+23/fits_headers.tar', f'wrong url {url}'
        assert fqn == '/tmp/fits_headers.tar', f'wrong url {fqn}'
        shutil.copy(f'{test_data_dir}/P000+23/fits_headers.tar', '/tmp')

    http_get_mock.side_effect = _http_get_mock

    test_subject = LOTSSDR2MetadataReader(clients_mock, test_config.http_get_timeout)
    assert test_subject is not None, 'ctor'

    test_mosaic_id = 'P000+23'
    test_storage_name = LOTSSName(test_mosaic_id)
    test_subject.set(test_storage_name)

    assert len(test_subject.headers) == 8, f'wrong header length {len(test_subject.headers)}'
    assert len(test_storage_name.destination_uris) == 8, f'wrong uris len {test_storage_name}'

    test_uri_prefix = f'{test_storage_name.scheme}:{test_storage_name.collection}/{test_mosaic_id}/'
    for check in [test_storage_name.destination_uris, test_subject.headers.keys()]:
        assert f'{test_uri_prefix}low-mosaic-blanked.fits' in check, f'{check.__class__.__name__} low-mosaic-blanked'
        assert f'{test_uri_prefix}low-mosaic-weights.fits' in check, f'{check.__class__.__name__} low-mosaic-weights'
        assert f'{test_uri_prefix}mosaic-blanked.fits' in check, f'{check.__class__.__name__} mosaic-blanked'
        assert f'{test_uri_prefix}mosaic-weights.fits' in check, f'{check.__class__.__name__} mosaic-weights'
        assert f'{test_uri_prefix}mosaic.pybdsmmask.fits' in check, f'{check.__class__.__name__} mosaic.pybdsmmask'
        assert f'{test_uri_prefix}mosaic.resid.fits' in check, f'{check.__class__.__name__} mosaic.resid'
        assert f'{test_uri_prefix}mosaic-rms.fits' in check, f'{check.__class__.__name__} mosaic.rms'
