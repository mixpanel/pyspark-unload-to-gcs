"""Mock pyspark and dbutils for testing without the heavy dependency."""

import builtins
import sys
from unittest.mock import MagicMock

sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()
sys.modules["pyspark.sql.functions"] = MagicMock()

mock_dbutils = MagicMock()
builtins.dbutils = mock_dbutils  # type: ignore[attr-defined]
