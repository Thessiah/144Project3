This example contains a simple utility class to simplify opening database
connections in Java applications, such as the one you will write to build
your Lucene index. 

To build and run the sample code, use the "run" ant target inside
the directory with build.xml by typing "ant run".

The indexes used are ItemID, ItemName, Category, and Description. These
are the only real indexes necessary. There's also a Content index which is
simply the union of all 3 of these (excludes ID) for easy searching.