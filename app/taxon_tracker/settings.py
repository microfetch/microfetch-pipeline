from enum import Enum
import os


class Settings(Enum):
    TAXON_UPDATE_N = int(os.environ.get('TAXON_UPDATE_N', '7'))
    TAXON_UPDATE_UNITS = os.environ.get('TAXON_UPDATE_UNITS', 'days')
    CONSIDERATION_PERIOD_N = int(os.environ.get('CONSIDERATION_PERIOD_N', '10'))
    CONSIDERATION_PERIOD_UNITS = os.environ.get('CONSIDERATION_PERIOD_UNITS', 'minutes')
    ASSEMBLY_PERIOD_N = int(os.environ.get('ASSEMBLY_PERIOD_N', '7'))
    ASSEMBLY_PERIOD_UNITS = os.environ.get('ASSEMBLY_PERIOD_UNITS', 'days')
    ENA_REQUEST_LIMIT = int(os.environ.get('ENA_REQUEST_LIMIT', '1000'))
    MAX_DROPLETS = int(os.environ.get('MAX_DROPLETS', '10'))
