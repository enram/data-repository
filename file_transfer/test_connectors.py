
import pytest

from .connectors import LocalConnector


@pytest.fixture
def getlocaldir(tmpdir):
    be_base = tmpdir.mkdir("be")
    be = be_base.mkdir("jab").mkdir("2016").mkdir("11").mkdir("20")
    be22 = be.mkdir("22")
    be23 = be.mkdir("23")
    be22.join("bejab_vp_20161120225000.h5").write("bejab_vp_20161120225000")
    be22.join("bejab_vp_20161120223000.h5").write("bejab_vp_20161120223000")
    be23.join("bejab_vp_20161120235000.h5").write("bejab_vp_20161120235000")
    be23.join("bejab_vp_20161120233000.h5").write("bejab_vp_20161120233000")

    no = tmpdir.mkdir("no").mkdir("hgb").mkdir("2016").mkdir("11").mkdir("20")
    no8 = no.mkdir("08")
    no8.join("nohgb_vp_20161120083100.h5").write("nohgb_vp_20161120083100")
    no8.join("nohgb_vp_20161120082200.h5").write("nohgb_vp_20161120082200")

    return tmpdir, be_base, be, no


class TestLocalConnect:

    def test_local_file_list(self, getlocaldir):
        tmpdir, be_base, be, no = getlocaldir
        localclient = LocalConnector(tmpdir.dirname)

        files = list(localclient.list_files(path=None,
                                            name_match="_vp_",
                                            fullpaths=False))
        expect_files = ["bejab_vp_20161120225000.h5",
                        "bejab_vp_20161120223000.h5",
                        "bejab_vp_20161120235000.h5",
                        "bejab_vp_20161120233000.h5",
                        "nohgb_vp_20161120083100.h5",
                        "nohgb_vp_20161120082200.h5"
                        ]
        assert set(files) == set(expect_files)

        # sub folder
        localclient = LocalConnector(be_base.dirname)
        files = list(localclient.list_files(path="be/jab/2016/11/20/22",
                                            name_match="_vp_",
                                            fullpaths=False))
        expect_files = ["bejab_vp_20161120225000.h5",
                        "bejab_vp_20161120223000.h5"]
        assert set(files) == set(expect_files)
