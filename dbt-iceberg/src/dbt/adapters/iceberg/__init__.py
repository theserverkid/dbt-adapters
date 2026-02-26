from dbt.adapters.iceberg.connections import SparkConnectionManager  # noqa
from dbt.adapters.iceberg.connections import SparkCredentials
from dbt.adapters.iceberg.relation import SparkRelation  # noqa
from dbt.adapters.iceberg.column import SparkColumn  # noqa
from dbt.adapters.iceberg.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import iceberg

Plugin = AdapterPlugin(
    adapter=SparkAdapter,  # type:ignore
    credentials=SparkCredentials,
    include_path=iceberg.PACKAGE_PATH,
)
