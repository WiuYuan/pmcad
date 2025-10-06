# PMCAD

Using article abstract database from pubmed, combined with bioinformatics tools to construct new database


## Install dependency for C++

```{bash}
bash install_dependency.sh
```

## Set up pybind for C++ and python

```{bash}
bash build.sh
```

## database structure

Suppose final database in $DATABASEPATH, then $DATABASEPATH/data is the postgresql data base, $DATABASEPATH/database.info store the basic information

## insert files into database example

```{python}
python build-database-example.py
```