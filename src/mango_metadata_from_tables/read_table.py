import pandas as pd
from pathlib import Path
from irods.exception import DataObjectDoesNotExist, CollectionDoesNotExist
from irods.data_object import iRODSDataObject


def create_file_object(path: str, session=None):
    """Turn path to file into a file-like object.

    Args:
        session: (iRODSSession or None): session to connect to iRODS.
            Session can be set to None for testing non-irods functionalities.
        path (str): Path to tabular file

    Raises:
        FileNotFoundError: If the file cannot be found locally or in ManGO.

    Returns:
        pathlib.Path or irods.iRODSDataObject: File-like object to read metadata from.
    """
    ppath = Path(path)
    if ppath.suffix not in [".xlsx", ".csv", ".tsv"]:
        raise IOError("Filetype not accepted")
    if ppath.exists():
        return ppath
    if session:
        try:
            return session.data_objects.get(path)
        except DataObjectDoesNotExist or CollectionDoesNotExist as e:
            raise e
    raise FileNotFoundError


def parse_tabular_file(path: str, session=None, separator: str = ","):
    """Parse tabular file.

    Args:
        path (str): Path to the tabular file.
        session (iRODSSession or None): session to connect to iRODS.
            If it is none, it is assumed that we are testing
        separator (str, optional): Separator for plain text files.. Defaults to ",".

    Raises:
        IOError: If the file cannot be parsed (it is not .xlsx, .csv or .tsv) it won't be read.

    Returns:
        dict: Dictionary of pandas.DataFrames with sheet names as keys.
    """

    file = create_file_object(path, session)
    if path.endswith("xlsx"):
        # Local excel files are binary and should be opened with 'rb'.
        # However, iRODS implemented their 'open' method differently,
        # and there you should use just 'r' instead
        reading_mode = "r" if isinstance(file, iRODSDataObject) else "rb"
        with file.open(reading_mode) as f:
            sheets = pd.read_excel(f, sheet_name=None)
        if any(x.strip() != x for x in sheets.keys()):
            sheets = {k.strip(): v for k, v in sheets.items()}
    else:
        # these types are not binary and should be opened with 'r'
        with file.open("r") as f:
            sheets = {"single_sheet": pd.read_csv(f, sep=separator)}
    for sheet in sheets.values():
        sheet.columns = sheet.columns.str.strip()
    return sheets
