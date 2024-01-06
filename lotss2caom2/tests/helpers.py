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

import numpy as np
from astropy.table import Table


def _search_id_list_mock(ignore_query):
    # the query return value is pyvo.dal.tap.TAPResults, but for the purposes of testing
    # astropy.table.Table has the same behaviour
    return Table.read(
        'mosaic_id\n' 'P000+23\n' 'P000+31\n' 'P000+36\n'.split('\n'),
        format='csv',
    )


# 'https://vo.astron.nl/getproduct/LoTSS-DR2/P000%2B23\t\t\timage/fits\t639106560\t0.03125\t23.3953\tP000+23_mosaic-blanked.fits\tLOFAR.HBA\t58452.6351273148\t2\tmasked_array(data=[8938, 8938]\t masked_array(data=[0.0004166669968981296,0.0004166669968981296]\tICRS\t2000.0\tSIN\tmasked_array(data=[4469.0, 4469.0]\tmasked_array(data=[0.03125, 23.3953]\tmasked_array(data=[-0.0004166669968981296, 0.0, 0.0, 0.0004166669968981296]\t120-168MHz\tm\t2.08189\t2.49827\t1.78448\tZ\tmasked_array(data=[2.032831406303902, 21.520385451900815, 2.08992790594909, 25.244422592409883, 357.9721112359749, 25.24441664211163, 358.02922050953805, 21.520379666564615]\tP000+23\thttps://vo.astron.nl/lotss_dr2/q/dlmosaic/dlmeta?ID=ivo%3A//astron.nl/%7E%3FLoTSS-DR2/P000%2B23\tmasked_array(data=[689778]\t21.12136/ede3eff0-266a-465a-af68-6f292391485f\t8eefe6e9\n'.split('\n'),
def _search_mosaic_id_mock(query_string):
    # the query return value is pyvo.dal.tap.TAPResults, and the individual masked arrays are numpy.ma.core.MaskedArray
    # but for the purposes of testing a list of dicts has the same behaviour
    if 'P000' in query_string:
        result = [
            {
                'accref': 'https://vo.astron.nl/getproduct/LoTSS-DR2/P000%2B23',
                'owner': '',
                'embargo': '',
                'mime': 'image/fits',
                'accsize': np.int64(639106560),
                'centerAlpha': 0.03125,
                'centerDelta': 23.3953,
                'imageTitle': 'P000+23_mosaic-blanked.fits',
                'instid': 'LOFAR.HBA',
                'dateobs': 58452.6351273148,
                'nAxes': 2,
                'pixelsize': np.array([8938, 8938]),
                'pixelScale': np.array([0.0004166669968981296, 0.0004166669968981296]),
                'refframe': 'ICRS',
                'wcs_equinox': 2000.0,
                'wcs_projection': 'SIN',
                'wcs_refpixel': np.array([4469.0, 4469.0]),
                'wcs_refvalues': np.array([0.03125, 23.3953]),
                'wcs_cdmatrix': np.array([-0.0004166669968981296, 0.0, 0.0, 0.0004166669968981296]),
                'bandpassid': '120-168MHz',
                'bandpassunit': 'm',
                'bandpassrefval': 2.08189,
                'bandpasshi': 2.49827,
                'bandpasslo': 1.78448,
                'pixflags': 'Z',
                'coverage': np.array(
                    [
                        2.032831406303902,
                        21.520385451900815,
                        2.08992790594909,
                        25.244422592409883,
                        357.9721112359749,
                        25.24441664211163,
                        358.02922050953805,
                        21.520379666564615,
                    ]
                ),
                'mosaic_id': 'P000+23',
                'related_products': 'https://vo.astron.nl/lotss_dr2/q/dlmosaic/dlmeta?ID=ivo%3A//astron.nl/%7E%3FLoTSS-DR2/P000%2B23',
                'lofar_obsids': np.array([689778]),
                'data_pid': '21.12136/ede3eff0-266a-465a-af68-6f292391485f',
                'adler32': '8eefe6e9',
            }
        ]
    else:
        result = [
            {
                'accref': 'https://vo.astron.nl/getproduct/LoTSS-DR2/P124%2B62',
                'owner': '',
                'embargo': '',
                'mime': 'image/fits',
                'accsize': np.int64(686577600),
                'centerAlpha': 124.678,
                'centerDelta': 61.9895,
                'imageTitle': 'P124+62_mosaic-blanked.fits',
                'instid': 'LOFAR.HBA',
                'dateobs': 58350.241377315,
                'nAxes': 2,
                'pixelsize': np.array([9264, 9264]),
                'pixelScale': np.array([0.0004166669968981296, 0.0004166669968981296]),
                'refframe': 'ICRS',
                'wcs_equinox': 2000.0,
                'wcs_projection': 'SIN',
                'wcs_refpixel': np.array([4632.0, 4632.0]),
                'wcs_refvalues': np.array([124.678, 61.9895]),
                'wcs_cdmatrix': np.array([-0.0004166669968981296, 0.0, 0.0, 0.0004166669968981296]),
                'bandpassid': '120-168MHz',
                'bandpassunit': 'm',
                'bandpassrefval': 2.08189,
                'bandpasshi': 2.49827,
                'bandpasslo': 1.78448,
                'pixflags': 'Z',
                'coverage': np.array(
                    [
                        128.5403335917227,
                        60.00207770393527,
                        129.06122140208868,
                        63.85463521484545,
                        120.2938346473764,
                        63.85460706549431,
                        120.81483402706476,
                        60.002052894353966,
                    ]
                ),
                'mosaic_id': 'P124+62',
                'related_products': 'https://vo.astron.nl/lotss_dr2/q/dlmosaic/dlmeta?ID=ivo%3A//astron.nl/%7E%3FLoTSS-DR2/P124%2B62',
                'lofar_obsids': np.array([664568]),
                'data_pid': '21.12136/462dc630-a03c-40e9-b902-1f61b2522f42',
                'adler32': '865c7c66',
            }
        ]
    return result
