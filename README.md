# GVT Scripts

Multiple scripts for the GVT

## Instalation

You can install these scripts using the [pip](https://pip.pypa.io/) tool in Python, directly from GitHub.

```bash
$ pip install -U https://github.com/leliel12/gvt_scripts/archive/refs/heads/master.zip
```

If you want to use Anaconda, first install *pip* with [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).


```bash
$ conda install pip
```


## `shp-metadata` command

here's a tutorial for using the `shp-metadata` sub-commands command-line interface (CLI):

### `$ shp-metadata show`

Usage:

```bash
$ shp-metadata show --path <path_to_zipped_shp_file>
```

This command takes the path to a zipped shapefile and prints its metadata.

### `$ shp-metadata export`

Usage:

```bash
$ shp-metadata export <path_to_zipped_shp_file> --to <output_file_path>
```

This command takes the *path* to a zipped shapefile and exports its metadata to the specified output file, with the format specified by the *to* extension.

Available formats `{'.toml', '.xml', '.yaml', '.json'}`.
