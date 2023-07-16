# heatwave-core

This is the core application to calculate heatwaves.

## Design Considerations

* As the requirements point out a preference of Apache Spark, it is used for the algorithm implementation.
* As it is usually easier to reason about code at a higher level of abstraction, the Spark SQL would be a sound choice.
  Due to the requirements state that Spark SQL may not be leveraged, the RDD API is used.
* The use of the Spark RDD API prevents the implementation of the window functions-based algorithm. Hence, the
  array-based algorithm is used.
* As language Java is used. As Spark run in the JVM, the PySpark API are occasionally lagging behind in features. Java
  is also more performant. Neither of these arguments are important for this use case, so the choice is mostly
  arbitrary.
