import io
import os.path
import pathlib
from irods.session import iRODSSession
import click
from rich.markdown import Markdown
from rich.progress import track
from .dataframe2avus import dict_to_avus, generate_rows, apply_metadata_to_data_object
from .preprocessing import process_tabular_file, validate_schema_columns
from . import console

# region Chains


@click.command()
@click.option(
    "--config",
    type=click.File("r"),
    required=True,
    help="Configuration file created by `setup`.",
)
@click.option("--dry-run", is_flag=True, help="Simulate applying the metadata.")
@click.argument("filename")
def run(filename, config, dry_run=False):
    """Apply metadata from a tabular file to data objects.

    FILENAME is the path to the tabular file containing the metadata.
    It should have some column with a unique identifier for the data objects,
    and columns for other metadata fields.
    """

    tabular_file = pathlib.Path(filename)
    if not tabular_file.exists():
        raise FileExistsError
    try:
        env_file = os.environ["IRODS_ENVIRONMENT_FILE"]
    except KeyError:
        env_file = os.path.expanduser("~/.irods/irods_environment.json")

    ssl_settings = {}
    with iRODSSession(irods_env_file=env_file, **ssl_settings) as session:
        apply_metadata_from_table(tabular_file, config, dry_run, session)


def apply_metadata_from_table(
    filename: str | pathlib.Path,
    config: io.StringIO | click.File,
    dry_run: bool = False,
    session: iRODSSession | None = None,
):
    if session is None:
        console.print("No session found, we'll try a dry-run!")
    processed_config_data = process_tabular_file(filename, config, session)

    sheets = processed_config_data["sheets"]
    item_type = processed_config_data["item_type"]
    multivalue_columns = processed_config_data["multivalue_columns"]
    multivalue_separator = processed_config_data["multivalue_separator"]
    schema_instructions = processed_config_data["schema_instructions"]
    sheets_for_schemas = validate_schema_columns(
        sheets, schema_instructions.get("schema", None)
    )
    for sheetname, sheet in sheets.items():
        sheet_schema_instructions = (
            schema_instructions if sheetname in sheets_for_schemas else {}
        )
        progress_message = f"Adding metadata from {sheetname + ' in ' if len(sheets) > 1 else ''}`{filename}`..."
        n = 0
        errors = 0
        min_avus = None
        max_avus = None

        # loop over each row printing a progress bar
        for dataobject, md_dict in track(
            generate_rows(sheet, multivalue_columns, multivalue_separator),
            description=progress_message,
        ):
            if session and not dry_run:
                avus = apply_metadata_to_data_object(
                    dataobject, md_dict, sheet_schema_instructions, session, item_type
                )
                yield {"dataobject": dataobject, "avus": avus}
            else:
                console.print(
                    f"Creating the following AVUs for {item_type.value} {dataobject}:"
                )
                avus = dict_to_avus(md_dict, **sheet_schema_instructions)
                print(avus)
                yield {"dataobject": dataobject, "avus": avus}
            if avus:
                n += 1
                if max_avus is None or len(avus) > max_avus:
                    max_avus = len(avus)
                if min_avus is None or len(avus) < min_avus:
                    min_avus = len(avus)
            else:
                errors += 1

        avu_length_range = (
            max_avus if min_avus == max_avus else f"{min_avus} to {max_avus}"
        )
        console.print(
            Markdown(
                # This calculation may not be correct anymore in case of multiple values,
                # since the md_dict of each object can now have a different length
                f"{'Simulated' if dry_run else 'Applied'} {avu_length_range} AVUs for each of {n} {item_type.value}s"
            )
        )
        if errors > 0:
            console.print(
                f"{errors} data objects were skipped because the paths were not valid!",
                style="red bold",
            )
