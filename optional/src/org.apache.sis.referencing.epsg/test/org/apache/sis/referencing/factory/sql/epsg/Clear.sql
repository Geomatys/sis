--
-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
-- http://www.apache.org/licenses/LICENSE-2.0
--


--
-- Delete all EPSG tables. Useful for repeated tests of data insertions.
--

DROP TABLE epsg_alias CASCADE;
DROP TABLE epsg_change CASCADE;
DROP TABLE epsg_conventionalrs CASCADE;
DROP TABLE epsg_coordinateaxis CASCADE;
DROP TABLE epsg_coordinateaxisname CASCADE;
DROP TABLE epsg_coordinatereferencesystem CASCADE;
DROP TABLE epsg_coordinatesystem CASCADE;
DROP TABLE epsg_coordoperation CASCADE;
DROP TABLE epsg_coordoperationmethod CASCADE;
DROP TABLE epsg_coordoperationparam CASCADE;
DROP TABLE epsg_coordoperationparamusage CASCADE;
DROP TABLE epsg_coordoperationparamvalue CASCADE;
DROP TABLE epsg_coordoperationpath CASCADE;
DROP TABLE epsg_datum CASCADE;
DROP TABLE epsg_datumensemble CASCADE;
DROP TABLE epsg_datumensemblemember CASCADE;
DROP TABLE epsg_datumrealizationmethod CASCADE;
DROP TABLE epsg_definingoperation CASCADE;
DROP TABLE epsg_deprecation CASCADE;
DROP TABLE epsg_ellipsoid CASCADE;
DROP TABLE epsg_extent CASCADE;
DROP TABLE epsg_namingsystem CASCADE;
DROP TABLE epsg_primemeridian CASCADE;
DROP TABLE epsg_scope CASCADE;
DROP TABLE epsg_supersession CASCADE;
DROP TABLE epsg_unitofmeasure CASCADE;
DROP TABLE epsg_usage CASCADE;
DROP TABLE epsg_versionhistory CASCADE;
DROP TYPE  epsg_datum_kind CASCADE;
DROP TYPE  epsg_crs_kind CASCADE;
DROP TYPE  epsg_cs_kind CASCADE;
DROP TYPE  epsg_table_name CASCADE;
