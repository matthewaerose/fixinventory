from typing import Optional, Any, AsyncIterator, List

from fixcore.cli.model import CLICommand, CLIContext, EmptyContext, CLIAction, ArgsInfo, CLISource
from fixcore.db.model import QueryModel
from fixcore.dependencies import TenantDependencies
from fixcore.model.graph_access import Section
from fixcore.query import Query
from fixcore.query.model import P


class IdxCommand(CLICommand):
    @property
    def name(self) -> str:
        return "idx"

    def args_info(self) -> ArgsInfo:
        return []

    def info(self) -> str:
        return ""

    def parse(self, arg: Optional[str] = None, ctx: CLIContext = EmptyContext, **kwargs: Any) -> CLIAction:
        props = arg.split(" ")

        async def run() -> AsyncIterator[str]:
            dba = self.dependencies.db_access
            sdb = dba.database
            vertex = sdb.collection(ctx.graph_name)
            indexes = {idx["name"] for idx in vertex.indexes()}
            section = ctx.section
            # db = dba.get_graph_db(ctx.graph_name)
            # model = await self.dependencies.model_handler.load_model(ctx.graph_name)
            for prop in props:
                idx_name = f"z_{prop}"
                # term = P.array(prop).for_any.ne(None) if prop.endswith("]") else P.single(prop).ne(None)
                # if idx_name not in indexes:
                #     async with await db.search_list(
                #         QueryModel(Query.by(term).on_section(Section.reported).with_limit(1), model)
                #     ) as cursor:
                #         if [item async for item in cursor]:
                if idx_name not in indexes:
                    try:
                        vertex.add_persistent_index(
                            [f"{section}.{prop}"], sparse=True, name=idx_name, in_background=False
                        )
                        yield f"Index created on {prop}"
                    except Exception as e:
                        yield f"Error creating index on {prop}: {e}"

        return CLISource.no_count(run)


def all_commands(d: TenantDependencies) -> List[CLICommand]:
    return [IdxCommand(d, "custom")]
