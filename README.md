[Link to site](https://tmoroe.github.io/)

## Polars is a powerful DataFrame library designed for efficient data manipulation and analysis. 
Here are some key points about Polars:

Performance-Driven: Polars is written from the ground up with performance in mind. Its multi-threaded query engine, implemented in Rust, is designed for effective parallelism. The vectorized and columnar processing enables cache-coherent algorithms, resulting in high performance on modern processors.

Easy to Use: If you’re familiar with data wrangling, you’ll feel right at home with Polars. Its intuitive expressions empower you to write readable and performant code simultaneously.
Open Source: Polars is and always will be open source. An active community of developers contributes to its development, and it’s free to use under the MIT license.

Benchmark Performance: Polars was benchmarked against several other solutions using the independent TPC-H Benchmark, which replicates data wrangling operations used in practice. Compared to pandas, Polars achieves more than 30x performance gains. It’s lightning-fast and suitable for large-scale data frames.

Supported Formats: Polars supports reading and writing to various data formats, including CSV, JSON, Parquet, Delta Lake, AVRO, Excel, Feather, Arrow, MySQL, Postgres, SQL Server, Sqlite, Redshift, Oracle, S3, Azure Blob, and Azure File