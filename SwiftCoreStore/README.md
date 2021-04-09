# A Warning

This subdirectory is strange. Thanks, Apple, I guess?

The problem is, CoreStore and its dependency CoreData is only available
on Apple platforms (macOS, iOS, etc.), not on Linux. Therefore, the
actual database/ORM querying example must be run on a macOS computer.

However, on macOS, the Swift Package Manager does not work without Xcode,
i.e. with a stand-alone installation of the Command Line Tools. The actual
Xcode GUI itself has to be installed in order for the `swift build` etc.
commands to work.

However, I haven't got sufficient resources (or time, or willingness) to
install Xcode, therefore the Swift Package Manager does not work on macOS.
I worked around this issue by adding the CoreStore framework as a git
submodule, and wrote a Makefile that manually compiles that library along
with the CoreStore/CoreData example queries.

However, this is not really feasible in the case of the complexity metrics
module. Coming up with the complexity metrics requires parsing Swift code,
which is easily done using the SwiftSyntax package. Unfortunately, this
package depends on FFI (it actually calls into the Swift compiler which is
written in C++), and so compiling it is quite involved. Therefore, I opted
for using the Swift package manager anyway — which, however, means that the
part of the software measuring complexity metrics only compiles on Linux,
and it does not compile on macOS.

Go figure.

