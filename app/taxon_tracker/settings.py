from enum import Enum

import os


class Settings(Enum):
    TAXON_UPDATE_N = int(os.environ.get('TAXON_UPDATE_N', '7'))
    TAXON_UPDATE_UNITS = os.environ.get('TAXON_UPDATE_UNITS', 'days')
    ASSEMBLY_PERIOD_N = int(os.environ.get('ASSEMBLY_PERIOD_N', '7'))
    ASSEMBLY_PERIOD_UNITS = os.environ.get('ASSEMBLY_PERIOD_UNITS', 'days')
    ENA_REQUEST_LIMIT = int(os.environ.get('ENA_REQUEST_LIMIT', '10'))
    EPOCH_DATE = os.environ.get('EPOCH_DATE', '2022-06-18')
