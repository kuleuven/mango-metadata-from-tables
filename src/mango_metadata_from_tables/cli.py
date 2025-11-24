import click
from .run import run
from .create_config import setup


# These are the functions that would be called from the command line :)
# (and use click)
@click.group()
def mdtab():
    """Process tabular files to add iRODS metadata to data objects."""
    pass


mdtab.add_command(setup)
mdtab.add_command(run)

# endregion

if __name__ == "__main__":
    mdtab()
