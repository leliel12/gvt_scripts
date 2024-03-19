# GVT Scripts

Multiple scripts for the GVT

## About the versioning
This project follows a semantic versioning know as **Calendar versioning**
With the format `YYYY.MM.DD[.MICRO]`

Where:
- `YYYY`: Four-digit year
- `MM`: Two-digit month
- `DD`: Two-digit day within the month
- `[.MICRO]`: Optional, for distinguishing multiple releases within the same day if needed

## Installation

You can install these scripts using the [pip](https://pip.pypa.io/) tool in Python, directly from GitHub.

```bash
$ pip install -U https://github.com/leliel12/gvt_scripts/archive/refs/heads/master.zip
```

If you want to use Anaconda, first install *pip* with [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).

```bash
$ conda install pip
```
---

## 1. The `search-shp-dir` command

Scandirectory of satellite observation create a indexed metadata db and
provide search capabilities.

Here's a tutorial for using the `search-shp-dir` sub-commands command-line interface (CLI):

### `$ search-shp-dir mkdb`

Usage:

```bash
$ search-shp-dir mkdb <path_to_directory> --db <database_file_path>
```

This command takes the path to a directory and creates a database with the specified output path.

Arguments:
- `PATH`: Path to the directory. [required]

Options:
- `--db PATH`: URL to the database file [required]

### `$ search-shp-dir ssearch`

Usage:

```bash
$ search-shp-dir ssearch --db <database_file_path> --query <search_query> [--to <output_file_path>]
```

This command performs a simple search over a database.

The query string should consist of one or more conditions in the format: "field operator value" separated by "&". Supported operators are: `"="`, `"!="`, `"<"`, `"<="`, `">"`, `">="`, `"in"`, `"not in"`.

One example of a query is "satellite = 'Landsat-8' & cloudperce <= 10"

The format is given by the "to" extension. Available formats: {`'.yml'`, `'.csv'`, `'.yaml'`, `'.xml'`, `'.json'`}

Options:
- `--db PATH`: URL to the database file [required]
- `--query TEXT`: Query to search for [required]
- `--to FILENAME`: Path to the output file

### `$ search-shp-dir fields`

Usage:

```bash
$ search-shp-dir fields
```

This command lists all the fields available in the database.

---

### `$ search-shp-dir fields-info`

Usage:

```bash
$ search-shp-dir fields-info <fields>... --db <database_file_path>
```

This command returns information about the type and possible values of given fields.

Arguments:
- `fields FIELDS...`: Fields names [required]

Options:
- `--db PATH`: URL to the database file [required]