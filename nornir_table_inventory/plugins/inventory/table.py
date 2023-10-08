import csv
import logging
import pathlib
from math import isnan
from typing import Any, Dict, List, Type, TypeVar, Optional

import pandas as pd
from nornir.core.inventory import (
    Inventory,
    Group,
    Groups,
    Host,
    Hosts,
    Defaults,
    ConnectionOptions,
    ParentGroups,
)

logger = logging.getLogger(__name__)

HostOrGroup = TypeVar("HostOrGroup", "Host", "Group")

def _empty(x: Any):
    """Checks if x is a NaN (not a number) or None/empty string"""
    return x is None or (isinstance(x, float) and isnan(x)) or x == ""


def _get_connection_options(data: Dict[str, Any]) -> Dict[str, ConnectionOptions]:
    cp = {}
    for cn, c in data.items():
        cp[cn] = ConnectionOptions(
            hostname=c.get("hostname"),
            port=c.get("port"),
            username=c.get("username"),
            password=c.get("password"),
            platform=c.get("platform"),
            extras=c.get("extras"),
        )
    return cp

def _get_data(
    data: Dict[str, Any], 
    isDefaults: Optional[bool] = False
) -> Dict[str, Any]:
    no_data_fields = ['name', 'hostname', 'port', 'username', 'password', 'platform']
    resp_data = {}
    netmiko_prefix = 'netmiko_'

    if isDefaults:
        no_data_fields.append('groups')

    for k, v in data.items():
        if (k not in no_data_fields) and (netmiko_prefix not in k):
            resp_data[k] = v if not _empty(v) else None
    return resp_data


def _get_host_netmiko_options(data: Dict[str, Any]) -> Dict[str, Any]:
    extra_opts = {}
    netmiko_options = {
        'netmiko': {
            'extras': {
            }
        }
    }
    """:cvar
    conn_timeout=5,
        auth_timeout=None,  # Timeout to wait for authentication response
        banner_timeout=15,  # Timeout to wait for the banner to be presented (post TCP-connect)
        # Other timeouts
        blocking_timeout=20,  # Read blocking timeout
        timeout=100,  # TCP connect timeout | overloaded to read-loop timeout
        session_timeout=60,  # Used for locking/sharing the connection
    
    
    """
    int_keys = 'timeout conn_timeout auth_timeout banner_timeout blocking_timeout session_timeout'.split()
    bool_keys = 'fast_cli'.split()
    netmiko_prefix = 'netmiko_'
    for k, v in data.items():
        if netmiko_prefix in k:
            new_k = k.replace(netmiko_prefix, '')

            if new_k in int_keys:
                extra_opts[new_k] = int(v)
            elif new_k in bool_keys:
                if str(v).lower() in ['0', 'false', 'none']:
                    extra_opts[new_k] = False
                else:
                    extra_opts[new_k] = True
            else:
                # if the value is nan,convert it to None
                if _empty(v):
                    extra_opts[new_k] = None
                else:
                    extra_opts[new_k] = v

    if extra_opts:
        netmiko_options['netmiko']['extras'] = extra_opts
        return _get_connection_options(netmiko_options)
    else:
        return {}

def _get_inventory_element(
    typ: Type[HostOrGroup], data: Dict[str, Any], defaults: Defaults
) -> HostOrGroup:
    # get keypoint data and convert to string or int
    name = data.get('name')
    hostname = data.get("hostname")
    port = data.get("port", 22)
    username = data.get("username")
    password = data.get("password")
    platform = data.get("platform")
    groups = data.get("groups")

    if name:
        name = str(name)
    if hostname:
        hostname = str(hostname) if not _empty(hostname) else None
    if port:
        port = int(port) if not _empty(port) else None
    if username:
        username = str(username) if not _empty(username) else None
    if password:
        password = str(password) if not _empty(password) else None
    if platform:
        platform = str(platform) if not _empty(platform) else None
    if groups:
        groups = [x for x in groups.split(",")]

    return typ(
        name=name,
        hostname=hostname,
        port=port,
        username=username,
        password=password,
        platform=platform,
        data=_get_data(data),
        groups=groups,
        defaults=defaults,
        connection_options=_get_host_netmiko_options(data),
    )

def _get_defaults(data: Dict[str, Any]) -> Defaults:
    name = data.get('name')
    hostname = data.get("hostname")
    port = data.get("port")
    username = data.get("username")
    password = data.get("password")
    platform = data.get("platform")

    if name:
        name = str(name)
    if hostname:
        hostname = str(hostname) if not _empty(hostname) else None
    if port:
        port = int(port) if not _empty(port) else None
    if username:
        username = str(username) if not _empty(username) else None
    if password:
        password = str(password) if not _empty(password) else None
    if platform:
        platform = str(platform) if not _empty(platform) else None

    return Defaults(
        hostname=data.get("hostname"),
        port=data.get("port"),
        username=data.get("username"),
        password=data.get("password"),
        platform=data.get("platform"),
        data=_get_data(data),
        connection_options=_get_host_netmiko_options(data),
    )

class FlatDataInventory:
    def __init__(
            self,
            hosts_data: List[Dict],
            groups_data: List[Dict],
            defaults_data: List[Dict],
    ) -> None:
        self.hosts_data = hosts_data
        self.groups_data = groups_data
        self.defaults_data = defaults_data
        
    def load(self) -> Inventory:
        defaults = Defaults()
        groups = Groups()
        hosts = Hosts()
        
        if self.defaults_data:
            defaults = _get_defaults(self.defaults_data)

        if self.groups_data:
            for g in self.groups_data:
                if not _empty(g['name']):
                    groups[g['name']] = _get_inventory_element(Group, g, defaults)
                else:
                    logger.error(f"HOST name is empty for data: {g}")
                    raise Exception('HOST name must not be empty')
                            
            for g in groups.values():
                g.groups = ParentGroups([groups[g] for g in g.groups])

        for h in self.hosts_data:
            if not _empty(h['name']):
                hosts[h['name']] = _get_inventory_element(Host, h, defaults)
            else:
                logger.error(f"HOST name is empty for data: {h}")
                raise Exception('HOST name must not be empty')
            
        for h in hosts.values():
            h.groups = ParentGroups([groups[g] for g in h.groups])

        return Inventory(hosts=hosts, groups=groups, defaults=defaults)


class CSVInventory(FlatDataInventory):
    def __init__(
            self,
            host_file: str = "hosts.csv",
            group_file: str = "groups.csv",
            defaults_file: str = "defaults.csv",
            encoding: str = "utf8"
    ) -> None:
        hosts_data = []
        groups_data = []
        defaults_data = {}

        host_file = pathlib.Path(host_file).expanduser()
        group_file = pathlib.Path(group_file).expanduser()
        defaults_file = pathlib.Path(defaults_file).expanduser()

        if defaults_file.exists():
            with open(defaults_file, "r", encoding=encoding) as f:
                for i in csv.DictReader(f):
                    defaults_data = i
                    break

        if group_file.exists():
            with open(group_file, 'r', encoding=encoding) as f:
                for i in csv.DictReader(f):
                    groups_data.append(i)

        with open(host_file, 'r', encoding=encoding) as f:
            for i in csv.DictReader(f):
                hosts_data.append(i)

        super().__init__(hosts_data=hosts_data, groups_data=groups_data, defaults_data=defaults_data)


class ExcelInventory(FlatDataInventory):
    def __init__(
            self,
            host_file: str = "hosts.xlsx",
            group_file: str = "groups.xlsx",
            defaults_file: str = "defaults.xlsx",
    ) -> None:
        hosts_data = []
        groups_data = []
        defaults_data = {}

        host_file = pathlib.Path(host_file).expanduser()
        group_file = pathlib.Path(group_file).expanduser()
        defaults_file = pathlib.Path(defaults_file).expanduser()

        if defaults_file.exists():
            dataframe = pd.read_excel(defaults_file)
            dataframe.fillna('')
            defaults_data = dataframe.to_dict(orient='records')

        if group_file.exists():
            dataframe = pd.read_excel(group_file)
            dataframe.fillna('')
            groups_data = dataframe.to_dict(orient='records')

        dataframe = pd.read_excel(host_file)
        dataframe.fillna('')
        hosts_data = dataframe.to_dict(orient='records')

        super().__init__(hosts_data=hosts_data, groups_data=groups_data, defaults_data=defaults_data)

if __name__ == '__main__':
    ...
