"""traffic_data_elt — shared ELT library for the Traffic Data ELT project.

Sub-packages
------------
config      Runtime settings loaded from environment variables.
extract     Source data readers (pNEUMA CSV parser).
load        Warehouse loaders (raw layer writer + audit).
transform   (reserved for future Python-level transformations)
utils       Shared helpers (logging, etc.)
"""

__version__ = "0.1.0"
