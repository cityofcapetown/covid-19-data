import io
import logging
import os
import urllib.parse

from requests_ntlm import HttpNtlmAuth
from shareplum import Site
from shareplum.site import Version


CITY_DOMAIN = "CAPETOWN"
CITY_PROXY = "internet.capetown.gov.za:8080"
PROXY_ENV_VARS = ["http_proxy", "https_proxy"]
PROXY_ENV_VARS = PROXY_ENV_VARS + list(map(lambda x: x.upper(), PROXY_ENV_VARS))


def get_auth_objects(username, password) -> (HttpNtlmAuth, str, dict):
    auth = HttpNtlmAuth(f'{CITY_DOMAIN}\\{username}', password)
    proxy_string = f'http://{username}:{password}@{CITY_PROXY}'
    proxy_dict = {
        "http": proxy_string,
        "https": proxy_string
    }

    return auth, proxy_string, proxy_dict


def set_env_proxy(proxy_string) -> None:
    for proxy_env_var in PROXY_ENV_VARS:
        logging.debug(f"Setting '{proxy_env_var}'")
        os.environ[proxy_env_var] = proxy_string


def get_sp_site(sp_domain, sp_site, auth) -> Site:
    site_string = urllib.parse.urljoin(sp_domain, sp_site)
    site = Site(site_string, auth=auth, version=Version.v2016)

    return site


def filter_site_list(site_list, file_name_pattern):
    file_list_dicts = site_list.GetListItems()

    file_list = (
        file_dict for file_dict in file_list_dicts
        if file_name_pattern in file_dict["Name"].lower()
    )

    for file_dict in file_list:
        yield file_dict


def get_sp_file(site, folder_path, file_name) -> bytes:
    sp_folder = site.Folder(folder_path)
    sp_file = sp_folder.get_file(file_name)

    return sp_file


def put_sp_file(site, folder_path, file_name, data):
    sp_folder = site.Folder(folder_path)

    sp_folder.upload_file(io.BytesIO(data), file_name)
