CREATE TABLE SpatialTable (ItemID INT PRIMARY KEY, Location GEOMETRY NOT NULL) ENGINE=MyISAM;

INSERT INTO SpatialTable (ItemID, Location) SELECT ItemID, POINT(Latitude, Longitude) FROM Items WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL;

CREATE SPATIAL INDEX SpatialIndex ON SpatialTable (Location);