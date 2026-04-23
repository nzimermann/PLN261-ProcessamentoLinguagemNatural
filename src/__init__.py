"""
PLN261 - Processamento de Linguagem Natural
Vetorização e clusterização de bebidas alcoólicas.
"""

__version__ = "0.1.0"

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)
