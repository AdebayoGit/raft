// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "RaftDB",
    platforms: [
        .iOS(.v14),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "RaftDB",
            targets: ["RaftDB"]
        ),
    ],
    targets: [
        .binaryTarget(
            name: "CRaftDB",
            path: "RaftDB.xcframework"
        ),
        .target(
            name: "RaftDB",
            dependencies: ["CRaftDB"],
            path: "Sources/RaftDB"
        ),
        .testTarget(
            name: "RaftDBTests",
            dependencies: ["RaftDB"],
            path: "Tests/RaftDBTests"
        ),
    ]
)
