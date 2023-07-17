# heatwave-core

This is the core application to calculate heatwaves.

heatwave calculates heat waves in the Netherlands following the KNMI
definition:
> A heat wave is a succession of at least 5 summer days (maximum temperature
> 25.0 °C or higher) in De Bilt, of which at least three are tropical days
> (maximum temperature 30.0 °C or higher).

## On Compiling, Testing, And Running heatwave

### Prerequisites

* JDK 17
* Maven 3.8.6 or greater

### Notes On Code Style

Checkstyle is bound to the Maven phase `validate` and configured to
error out in case of violations. The rule set used are the Google checks, which
can be found at `../config/google_checks.xml`.

You run Checkstyle locally with `mvn validate`.

### Compiling

Compile the code with `mvn clean compile`

### Testing

Tests are written with JUnit5. Run the tests with `mvn clean test`.

### Running The App

#### On The Local Machine

Run the app locally with

```bash
export MAVEN_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
mvn clean compile exec:java -Dexec.args="../data/*"
```

A path to the input files needs to be passed as CLI argument when running the
app.

The settings of MAVEN_OPTS is necessary, as Spark accesses
sun.nio.ch.DirectBuffer although access to this was restricted in Java 9. For
running spark locally, this JVM option needs to be passed. It is not required
when running the app with
spark-submit. [Source](https://stackoverflow.com/questions/10108374/maven-how-to-run-a-java-file-from-command-line-passing-arguments)

#### In A Docker Container

The Maven Assembly Plugin is bound to the phase`package`. The application can
be packaged as a fat executable jar by running `mvn package`.

Subsequently, a docker image can be built with the provided Dockerfile.

```bash
docker build -t michaelheinecke/heatwave .
```

And then run with

```bash
docker run -p 4040:4040 -it -v \
/Users/michael.heinecke/IdeaProjects/MichaelHeinecke/heatwave/data:/app/data \
michaelheinecke/heatwave /app/data
```

Note that the input files need to be mounted into the container for processing
using the -v flag. The command line argument `/app/data` points the application
to the folder in the container where the input data is located.

The Spark UI will be available at `localhost:4040`.

## Design Considerations

* As the requirements point out a preference of Apache Spark, it is used for
  the algorithm implementation.
* As it is usually easier to reason about code at a higher level of
  abstraction, the Spark SQL would be a sound choice. Due to the requirements
  state that Spark SQL may not be leveraged, the RDD
  API is used.
* The use of the Spark RDD API prevents the implementation of the window
  functions-based algorithm. Hence, the array-based algorithm is used. Refer to
  the discussion of the [algorithms](../algorithms) for details.
* As language Java is used. As Spark runs in the JVM, the PySpark API are
  occasionally lagging behind in features. Java is also more performant.
  Neither of these arguments are important for this use case, so the choice is
  mostly arbitrary.
