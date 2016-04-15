hadoop jar .\target\ru.fj.meteodata-0.0.1-SNAPSHOT.jar
         ru.fj.meteodata.avg.AvgJob <temperature.txt> <out>

hadoop jar .\target\ru.fj.meteodata-0.0.1-SNAPSHOT.jar
         ru.fj.meteodata.join.reduce.ReduceJoinJob <temperature.txt> <stationLocation.txt> <out>

hadoop jar .\target\ru.fj.meteodata-0.0.1-SNAPSHOT.jar
         ru.fj.meteodata.join.map.CompositeMapSideJoin <temperature.txt> <stationLocation.txt> <out>

hadoop jar .\target\ru.fj.meteodata-0.0.1-SNAPSHOT.jar
         ru.fj.meteodata.join.map.cache.MapSideJoinCacheJob <temperature.txt> <stationLocation.txt> <out>