# import asyncio
# import itertools
# import sys
# from typing import List
#
# from scone.head import Head, Recipe
# from scone.head.kitchen import Kitchen
# from scone.head.recipe import Preparation

# def main(args=None):
#     if args is None:
#         args = sys.argv[1:]
#
#     if len(args) < 1:
#         raise RuntimeError("Needs to be passed a sous config directory as 1st arg!")
#
#     print("Am I a head?")
#
#     head = Head.open(args[0])
#
#     print(head.debug_info())
#
#     recipes_by_sous = head.construct_recipes()
#
#     all_recipes: List[Recipe] = list(
#         itertools.chain.from_iterable(recipes_by_sous.values())
#     )
#
#     prepare = Preparation(all_recipes)
#     order = prepare.prepare(head)
#
#     for epoch, items in enumerate(order):
#         print(f"----- Course {epoch} -----")
#
#         for item in items:
#             if isinstance(item, Recipe):
#                 print(f" > recipe {item}")
#             elif isinstance(item, tuple):
#                 kind, ident, extra = item
#                 print(f" - we now have {kind} {ident} {dict(extra)}")
#
#     print("Starting run")
#
#     k = Kitchen(head)
#
#     async def cook():
#         for epoch, epoch_items in enumerate(order):
#             print(f"Cooking Course {epoch} of {len(order)}")
#             await k.run_epoch(epoch_items)
#
#     asyncio.get_event_loop().run_until_complete(cook())
#
#
# if __name__ == "__main__":
#     main()
