"""Mock pyspark for testing without the heavy dependency."""

import sys
from unittest.mock import MagicMock

sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()
