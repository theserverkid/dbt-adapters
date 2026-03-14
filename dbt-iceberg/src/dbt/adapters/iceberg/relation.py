from typing import Optional, TypeVar
from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.events.logging import AdapterLogger

from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("Iceberg")

Self = TypeVar("Self", bound="BaseRelation")


@dataclass
class IcebergQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class IcebergIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class IcebergRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: IcebergQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: IcebergIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None
    require_alias: bool = False

    def render(self) -> str:
        return super().render()
