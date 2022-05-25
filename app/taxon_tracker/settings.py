from enum import Enum
import os


class Settings(Enum):
    TAXON_UPDATE_N = int(os.environ.get('TAXON_UPDATE_N', '7'))
    TAXON_UPDATE_UNITS = os.environ.get('TAXON_UPDATE_UNITS', 'days')
    ENA_REQUEST_LIMIT = int(os.environ.get('ENA_REQUEST_LIMIT', '1000'))
    MAX_DROPLETS = int(os.environ.get('MAX_DROPLETS', '10'))
