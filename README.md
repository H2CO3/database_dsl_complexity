# Supplementary Material for the paper "Query Complexity in Modern Database DSLs"

This repository contains source code, all data files, and all plots/figures which could not be included directly in the article.

The five subdirectories contain the implementation of the example schema introduced in the paper, along with the code implementing complexity measurements.

There is one exception among these, the `HaskellEsqueleto` directory, which contains a Haskell implementation using the Persistent and Esqueleto packages. However, this was only partially implemented and complexity measurements were not performed, but the code might still prove to be interesting to the reader.

Values of metrics, correlations, etc. are supplied in `.csv` tables for each language (subdirectory), and aggregated/summarized results can also be found in the repo root. All plots are provided in PDF format as well as the Python scripts that generated them. Python scripts assume they are invoked from their own containing directory, like `./script.py` or `python3 ./script.py`.

The source code is complete and runnable, provided that you install the necessary dependencies and have the appropriate toolchains:

 * A Python 3.9 distribution
 * .NET Core 5.0 CLI, with C# 9.0 support and NuGet (`dotnet build`)
 * Swift 5.3 toolchain with SwiftPM and the `clang` family of C and C++ compilers
 * A reasonably recent version of The Glasgow Haskell Compiler
 * MongoDB 4.4

**Do not forget to clone the repository using `git clone --recursive`!** Alternatively, if you are using an old version of Git, initialize submodules after cloning.

**Note:** Please see additional `README.md` files in subdirectories for more information and caveats.
