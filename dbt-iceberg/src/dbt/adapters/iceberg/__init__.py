from dbt.adapters.iceberg.connections import IcebergConnectionManager  # noqa
from dbt.adapters.iceberg.connections import IcebergCredentials
from dbt.adapters.iceberg.relation import IcebergRelation  # noqa
from dbt.adapters.iceberg.column import IcebergColumn  # noqa
from dbt.adapters.iceberg.impl import IcebergAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import iceberg

Plugin = AdapterPlugin(
    adapter=IcebergAdapter,  # type:ignore
    credentials=IcebergCredentials,
    include_path=iceberg.PACKAGE_PATH,
)
