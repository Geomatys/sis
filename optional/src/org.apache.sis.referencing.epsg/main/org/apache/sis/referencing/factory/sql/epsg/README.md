# Manual installation of EPSG data

This is the directory where EPSG data can be placed.
Those data are not commited in the Apache SIS source code repository because
they are licensed under [EPSG terms of use](https://epsg.org/terms-of-use.html).
For including the EPSG data in the `org.apache.sis.referencing.epsg` artifact,
the following commands must be executed manually with this directory as the
current directory:

```shell
# Execute the following in a separated directory.
svn checkout https://svn.apache.org/repos/asf/sis/data/non-free/
cd non-free
export NON_FREE_DIR=$PWD

# Execute the following in the directory of this `README.md` file.
ln --symbolic $NON_FREE_DIR/LICENSE.txt
ln --symbolic $NON_FREE_DIR/LICENSE.html
ln --symbolic $NON_FREE_DIR/Tables.sql
ln --symbolic $NON_FREE_DIR/Data.sql
ln --symbolic $NON_FREE_DIR/FKeys.sql
cd -
```

For removing the links (cleanup):

```shell
rm LICENSE.txt
rm LICENSE.html
rm Tables.sql
rm Data.sql
rm FKeys.sql
```
