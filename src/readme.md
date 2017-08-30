
Analysis
========
Images are loaded into HBase for flexibility of split management.

Tiles are written by the TileToHbase command

```
 java -cp target/original-osmosis-hbase-0.44.2.jar
  org.openstreetmap.osmosis.hbase.xyz.cmd.TileToHBase 
  -f ~/Classifv11/DisturbYear/Classif33yDisturbYear_CANorth_UL7E10N_LR42E0N-0000000000-0000000000.tif 
  -o 'ca_north'
```

```
java -cp target/original-osmosis-hbase-0.44.2.jar:target/dependency/* org.openstreetmap.osmosis.hbase.xyz.cmd.TileToHBase -f ~/Classifv11/DisturbYear/Classif33yDisturbYear_CANorth_UL7E10N_LR42E0N-0000000000-0000000000.tif -o 'ca_north'
```


Running:

```
hadoop jar target/osmosis-hbase-0.44.2.jar 
    org.openstreetmap.osmosis.hbase.mr.analysis.ImageRegions 
    ca_north /user/tempehu/classV11/stats7/ 2020
```


Bulk load
=========

Instead of preprocessing to seqence file, create the entities directly on HDFS?
Keeping all ways by default


Table design
============

Three primary data tables, nodes ways and relations are kept.  Each has the same row and rowkey design.

The rowkey is the primary key of the entity, with the byte order reversed.
With monotonically increasing keys, this creates an almost perfect distribution over the range 00 to FF for the first byte in the key (see KeyDistributionTest for an illustration).
When querying for known entity ids, e.g. when building a way, a node can be retrieved by simply reversing the known key.

*** A fourth table containing parsed entities is kept with a different strategy.
*** Populated using MR
*** the problem with invalid ways is avoided. They're not kept.
*** Nodes not included.
*** Polymorphic geometry types
*** It is secondary data after all, many ways are shared

Column family "d":
 * contains basic metadata, or common entity data (see org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData)

Column family "t":
 * Contains all tags in their own namespace.
 * Each tag key is a column, therefore standard query techniques e.g. Hive can be used with no fuss.

Tables should be created with the UniformSplit algorithm
hbase org.apache.hadoop.hbase.util.RegionSplitter nodes UniformSplit -c 30 -f d:t

Hive mapping
CREATE TABLE nodes(key int, value string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,t:name")
TBLPROPERTIES ("hbase.table.name" = "nodes");

CREATE external TABLE ways(key int, value string, geom binary)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,d:name,d:geom")
TBLPROPERTIES ("hbase.table.name" = "ways");


hbase org.apache.hadoop.hbase.util.RegionSplitter nodes UniformSplit -c 30 -f d:t
hadoop jar target/osmosis-hbase-0.1.jar org.openstreetmap.osmosis.hbase.mr.TableLoader ways /user/tempehu/africa-latest.pbf.seq /user/tempehu/hfile-relations
hdfs dfs -chmod -R 777 /user/tempehu/hfile-relations
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/tempehu/hfile-relations relations

Domain objects
==============
Currently the Osmosis domain has been used directly to facilitate integration with the rest of the toolchain.

Entities retrieved from HBase are eagerly evaluated. For example all tags are converted from binary map to a set of Tags (string pairs).
This is wasteful and the HBase representation is perhaps more useful - there's no reason the backing map in a result can't be kept as-is and tags evaluated lazily.
It may make sense to develop a higher performance version with a lazy wrapper.


*** Irritatingly we've had to use wrappers for each domain object.
*** These can't be constructed in a simple way and because Entity is a class we can't extend both this and the functionality in the domain objects.

Polygons, ways and relations
============================

The situation is a little complex, however:

Way polygons are relatively simple - if the start node is the same as the end node we have a simple polygon.
Polygon detection:
https://github.com/tyrasd/osm-polygon-features/blob/master/polygon-features.json

Multipolygons are messier:
http://wiki.openstreetmap.org/wiki/Relation:multipolygon


http://wiki.openstreetmap.org/wiki/Area/The_Future_of_Areas
http://wiki.openstreetmap.org/wiki/Relation:multipolygon
http://wiki.openstreetmap.org/wiki/Multipolygon_Examples
https://help.openstreetmap.org/questions/8273/how-do-i-extract-the-polygon-of-an-administrative-boundary
https://wiki.openstreetmap.org/wiki/Overpass_turbo/Polygon_Features
https://wiki.openstreetmap.org/wiki/Overpass_turbo/Polygon_Features

Design decision
===============
Given the need to add a qualifier to an HBase record (see table joins), it no longer really makes sense to separate tables.
It may be better to have a single entity table -- simplifies a lot of stuff - multi scans not necessary etc.

TODO
====
Single table branch
Tests for qualifiers in entity records
Protobuf for basic data records (user etc?)


