"""Domain-specific aiogram Routers.

Each router handles a specific feature domain.
Routers are registered in app.py via dp.include_routers().

Registration order matters — aiogram tries routers in order:
  common → wish → reminder → friend → gift → event
"""
