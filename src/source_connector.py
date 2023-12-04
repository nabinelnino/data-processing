"""
Base class for the iterators to build off
"""
import base64
import os
import copy

from abc import ABC


import yaml


class SourceConnector(ABC):
    """
    Base class responsible for defining the structure
    """

    def __init__(
        self,
        config_dict: dict,
        config_key: str = "connector_config",
        arguments_list=None,
    ):
        self.props: dict = {}

        self.configure_from_dict(copy.deepcopy(config_dict), config_key)

    def configure_from_dict(self, config_dict: dict, config_key: str) -> None:
        """
        Function for configuring the connector from a given config dict.
        :param config_dict: A dict with the required config information.
        :param config_key: The key in the provided dict that contains the config
        """
        try:
            self.props = config_dict[config_key]
        except KeyError as exc:
            raise KeyError(
                f"A '{config_key}' dictionary of values must be included in config file"
            ) from exc

    def prop(self, prop_name: str, optional=False, default_value=None):
        """
        Function for getting any property of this object from its internal
        props dictionary
        :param prop_name: the name of the property to be accessed
        """
        prop = None
        try:
            prop = self.props[prop_name]
        except KeyError as exc:
            if not optional and default_value is None:
                raise KeyError(
                    f"Property '{prop_name}' must be set in config file or before execution"
                ) from exc
            else:
                prop = default_value
        if prop is None:
            prop = default_value
        return prop
