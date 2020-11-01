#  Copyright 2020, Olivier 'reivilibre'.
#
#  This file is part of Scone.
#
#  Scone is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Scone is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Scone.  If not, see <https://www.gnu.org/licenses/>.

from pathlib import Path
from typing import Dict

from scone.head.dag import RecipeDag, RecipeState, Resource, Vertex
from scone.head.recipe import Recipe, recipe_name_getter

state_to_colour = {
    RecipeState.LOADED: ("white", "black"),
    RecipeState.PREPARED: ("azure", "black"),
    RecipeState.PENDING: ("pink", "black"),
    RecipeState.COOKABLE: ("gold", "black"),
    RecipeState.COOKED: ("darkolivegreen1", "black"),
    RecipeState.SKIPPED: ("cadetblue1", "black"),
    RecipeState.BEING_COOKED: ("darkorange1", "black"),
    RecipeState.FAILED: ("black", "orange"),
}


def emit_dot(dag: RecipeDag, path_out: Path) -> None:
    with open(path_out, "w") as fout:
        fout.write("digraph recipedag {\n")

        ids: Dict[Vertex, str] = dict()

        fout.write("\t// Vertices\n")

        for idx, vertex in enumerate(dag.vertices):
            vertex_id = f"v{idx}"
            ids[vertex] = vertex_id
            if isinstance(vertex, Recipe):
                rec_meta = dag.recipe_meta[vertex]
                label = (
                    f"{recipe_name_getter(vertex.__class__)}"
                    f" [{rec_meta.incoming_uncompleted}]"
                )
                colour, text_colour = state_to_colour[rec_meta.state]
                fout.write(
                    f'\t{vertex_id} [shape=box, label="{label}",'
                    f" style=filled, fontcolor={text_colour}, fillcolor={colour}];\n"
                )
            elif isinstance(vertex, Resource):
                label = str(vertex).replace("\\", "\\\\").replace('"', '\\"')
                res_meta = dag.resource_meta[vertex]
                colour = "darkolivegreen1" if res_meta.completed else "pink"
                fout.write(
                    f'\t{vertex_id} [label="{label}",'
                    f" style=filled, fillcolor={colour}];\n"
                )
            else:
                raise ValueError(f"? vertex {vertex!r}")

        fout.write("\n\t// Edges\n")

        for from_vert, edges in dag.edges.items():
            for to_vert in edges:
                fout.write(f"\t{ids[from_vert]} -> {ids[to_vert]};\n")

        fout.write("}\n")
