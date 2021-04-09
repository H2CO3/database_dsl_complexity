# Example Data for Demoing Database Abstraction Layers

The files in this directory contain, in JSON format, a complete (albeit small) database,
filled with semi-realistic data, which hopefully serves the following goals:

* Demonstrating the strengths and weaknesses of different Database Abstraction Layers
  related to various languages and technologies;
* Comparing the output of queries to each other and to a pre-defined set of gold standards,
  so as to make sure that the different demos perform semantically identical tasks;
* Compare two data sets corresponding to different schemas, in order to test the migration
  capabilities of various DALs as well as our own work.

The following criteria have been respected while compiling the data sets:

* Data should be organized so that it is easy to parse and transform in each language.
  The JSON format is ideal for this purpose.
* The contained data should represent and expose real-world problems to the widest possible
  extent. To that end, the following potential sources of complexity and errors are actively
  added to the data sets:
  * Identically-valued raw IDs across collections (for testing type safety w.r.t. entities)
  * 0, 1, or multiple results from a query
  * 0, 1, or multiple affected rows in an update or deletion
  * Possibility for successful and failing deletions and updates
  * Different hierarchies
    * Hierarchies with 0, 1, or more levels
    * Hierarchies that contain 0 roots (empty), 1 root (tree), or many roots (forest)
    * Hierarchies that contain direct recursion (child or parent pointer points into
      the same entity type)
    * Hierarchies that contain indirect, mutual recursion (child or parent pointer
      points into a _different_ entity type)
  * Migration (Difference between the two schemas) has to perform every possible kind of change:
    * Addition of a new type (`struct` and `enum` as well)
    * Deletion of a type
    * Renaming of a type
    * Addition of a field to a `struct` type
    * Deletion of a field from a `struct` type
    * Renaming of a field in a `struct` type
    * Changing the type of a field in a `Struct` type
    * Addition of a variant to an `enum` type
    * Deletion of a variant from an `enum` type
    * Renaming of a variant in an `enum` type
    * Changing the associated data type of a variant in an `enum` type
    * Changing the cardinality of a relationship, in all possible combinations of
      `(1-to-1, 1-to-N, M-to-N)` (Cartesian product with itself)
      * Changing the optionality of a relation (1 is optional or not (`?`), N and M
        is at least 0 or at least 1 (`*` or `+`))
  * Some UPDATEs have the potential of bringing the database to an inconsistent state
    either with respect to typed-ness, key and other constraints, or "only" rules of
    the logic of the edomain model. This allows for testing the `row_version` canary
    mechanism (if it exists).
    * E.g.: two clients simultaneously trying to buy the last piece of a product
