"""@bruin
name: ingestion.income_quintile
type: python
@bruin"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from utils import ingest_eurostat

ingest_eurostat("tessi180", "income_quintile")
