## Data modeling concepts
Data modeling is a process that involves identifying the entities (items to be stored) and the relationships between entities.

Data modeling in Cassandra uses a query-driven approach, in which specific queries are the key to organizing the data. Queries are the result of selecting data from a table; schema is the definition of how data in the table is arranged.
Cassandra's database design is based on the requirement for fast reads and writes, so the better the schema design, the faster data is written and retrieved.
cyclist_names
cyclist_category

Data in Cassandra is often arranged as one query per table, and data is repeated amongst many tables, a process known as denormalization.

Question: A case of n keywords stored in m rows is not a good table design, Why?
```$xslt
CREATE TABLE cyclist_mv (cid UUID PRIMARY KEY, name text, age int, birthday date, country text);
```
## Primary Key
```sql
CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 };
```
A primary key identifies the location and order of stored data. The primary key is defined when the table is created and cannot be altered. If you must change the primary key, create a new table schema and write the existing data to the new table.

```sql
CREATE TABLE cycling.cyclist_alt_stats ( id UUID PRIMARY KEY, lastname text, birthday timestamp, nationality text, weight text, height text );
```
CQL supports the following collection column types: map, set, and list.
```sql
CREATE TABLE cycling.whimsey ( id UUID PRIMARY KEY, lastname text, cyclist_teams set<text>, events list<text>, teams map<int,text> );
```
Collection types cannot be nested. Collections can include frozen data types.
```sql
CREATE TABLE cycling.route (race_id int, race_name text, point_id int, lat_long tuple<text, tuple<float,float>>, PRIMARY KEY (race_id, point_id));
INSERT INTO cycling.race_winners ( race_name, race_position, cyclist_name ) VALUES (
  'National Championships South Africa WJ-ITT (CN)', 
  1, 
  {firstname:'Frances',lastname:'DU TOUT'}
);
CREATE TABLE cyclist_name ( id UUID PRIMARY KEY, lastname text, firstname text );

```
The primary key consists of only the partition key in this case. Data stored with a simple primary key will be fast to insert and retrieve if many values for the column can distribute the partitions across many nodes.

Composite partition keys are used when the data stored is too large to reside in a single partition. Using more than one column for the partition key breaks the data into chunks, or buckets. The data is still grouped, but in smaller chunks. This method can be effective if a Cassandra cluster experiences hotspotting, or congestion in writing data to one node repeatedly, because a partition is heavily writing. Cassandra is often used for time series data, and hotspotting can be a real issue. Breaking incoming data into buckets by year:month:day:hour, using four columns to route to a partition can decrease hotspots.


Data is retrieved using the partition key. Keep in mind that to retrieve data from the table, values for all columns defined in the partition key have to be supplied.

Cassandra stores an entire row of data on a node by partition key.

```sql
cqlsh> USE cycling;
CREATE TABLE rank_by_year_and_name ( 
race_year int, 
race_name text, 
cyclist_name text, 
rank int, 
PRIMARY KEY ((race_year, race_name), rank) 
);
```
 Clustering is a storage engine process that sorts data within each partition based on the definition of the clustering columns. Normally, columns are sorted in ascending alphabetical order.