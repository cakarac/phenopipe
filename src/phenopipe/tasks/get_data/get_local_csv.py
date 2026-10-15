import os
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import Field

from phenopipe.tasks.get_data.get_data import GetData


class GetLocalCsv(GetData):
    """Load a local or bucket CSV through the standard task completion pipeline."""

    path: str | Path
    from_bucket: bool = False
    csv_options: dict[str, Any] = Field(default_factory=dict)

    def _complete(self):
        if self.from_bucket:
            bucket_loc = (
                getattr(self.env_vars.get("query_conn"), "bucket_loc", None)
                or os.getenv("BUCKET_LOC")
                or os.getenv("WORKSPACE_BUCKET")
            )
            if not bucket_loc:
                raise ValueError("A bucket location is required when from_bucket=True")
            path = f"{str(bucket_loc).rstrip('/')}/{str(self.path).lstrip('/')}"
        else:
            path = Path(self.path).expanduser()
        if self.lazy:
            self.output = pl.scan_csv(path, **self.csv_options)
        else:
            self.output = pl.read_csv(path, **self.csv_options)