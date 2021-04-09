// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "ComplexityMetrics",
    dependencies: [
        .package(name: "SwiftSyntax",
                 url: "https://github.com/apple/swift-syntax.git",
                 .exact("0.50300.0")),
        .package(name: "CodableCSV",
                 url: "https://github.com/dehesa/CodableCSV.git",
                 .exact("0.6.5")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "ComplexityMetrics",
            dependencies: [
                "SwiftSyntax",
                "CodableCSV",
            ]),
        .testTarget(
            name: "ComplexityMetricsTests",
            dependencies: ["ComplexityMetrics"]),
    ]
)
