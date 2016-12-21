import pytest
import fd
import requests as r


def downloadable(resp):
    success_codes = [200, 301] # until they update their own links
    return resp.status_code in success_codes


def test_bls_cew_download():
    for url in fd.get_bls_cew_urls():
        assert downloadable(r.head(url))


def test_bls_cew_consolidate():
    assert False


def test_blw_ce_download():
    for url in fd.bls_ce['data_urls']:
        assert downloadable(r.head(fd.bls_ce['webpage']+url))
