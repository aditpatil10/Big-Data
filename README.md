# Project 1

The purpose of this project is to develop a simple Map-Reduce program on Hadoop that creates histograms of pixels
A pixel in an image can be represented using 3 colors: red, green, and blue, where each color intensity is an integer between 0 and 255. In this project, I was asked to write a Map-Reduce program that derives a histogram for each color. For red, for example, the histogram will indicate how many pixels in the dataset have a green value equal to 0, equal to 1, etc (256 values). The pixel file is a text file that has one text line for each pixel. For example, the line

23,140,45
represents a pixel with red=23, green=140, and blue=45.

# Project 2

The purpose of this project was to improve the performance of the program that creates histograms of pixels developed in Project 1 by using a combiner and in-mapper combining.

# Project 3

The purpose of this project is to develop a graph analysis program using Map-Reduce.
An undirected graph is represented in the input text file using one line per graph vertex. For example, the line

1,2,3,4,5,6,7
represents the vertex with ID 1, which is connected to the vertices with IDs 2, 3, 4, 5, 6, and 7. For example, the following graph:

is represented in the input file as follows:
3,2,1
2,4,3
1,3,4,6
5,6
6,5,7,1
0,8,9
4,2,1
8,0
9,0
7,6
My task was to write a Map-Reduce program that finds the connected components of any undirected graph and prints the size of these connected components. A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph. For the above graph, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7. Your program should print the sizes of these connected components: 3 and 7.

# Project 4

The purpose of this project is to develop a data analysis program using Apache Spark.
Implemented Project 1 in Scala and Spark.

# Project 5

The purpose of this project is to develop a graph analysis program using Apache Spark.
Implemented Project 3 in Scala and Spark

# Project 6

The purpose of this project is to develop a simple program using Apache Pig.
Implemented Project 1 using Apache Pig.

# Project 7

The purpose of this project is to develop a simple program using Apache Hive.
Implemented Project 1 using Apache Hive.

# Project 8

The purpose of this project is to develop a graph processing program using Pregel on Spark GraphX.
Implemented Project 3 using Pregel.

### Prerequisites
You will be needing java and maven installed. Along with it you will be needing Hadoop, Scala, Pig and Hive.
```


## Built With

* [Hadoop](https://hadoop.apache.org/)
* [Maven](https://maven.apache.org/) - Dependency Management
* [Java](https://jdk.java.net/) - 
* [Scala](https://spark.apache.org/)
* [Pig](https://pig.apache.org/)
* [Hive](https://hive.apache.org/) - 

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* All these projects were done in CSE6331 by Leonidas Fegaras at University of Texas at Arlington.
