"""
CLI that uses Typer to build the command line interface.

python cli.py --help

python cli.py all parquet
python cli.py mssql json
python cli.py postgres json
python cli.py mongodb json
python cli.py redis json
"""

import typer

from rich import print
from main import MinioStorage
from dotenv import load_dotenv

load_dotenv()


def main(dstype: str, format_type: str):
    """Write a file to the specified data storage.

    Args:
        dstype (str): The type of data storage to write the file to. It can be one of the following options: "internal",
                      "vendor", "all".
        format_type (str): The format of the file to be written.

    Returns:
        None

    This method writes a file to the specified data storage type based on the provided parameters. It uses the `MinioStorage`
    class to perform the file write operation.
    """

    if dstype == "internal":
        print(MinioStorage().write_file(ds_type="internal", format_type=format_type))
    elif dstype == "vendor":
        print(MinioStorage().write_file(ds_type="vendor", format_type=format_type))
    elif dstype == "all":
        print(MinioStorage().write_file(ds_type="internal", format_type=format_type))
        print(MinioStorage().write_file(ds_type="vendor", format_type=format_type))

if __name__ == "__main__":
    typer.run(main)
