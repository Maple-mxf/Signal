rootProject.name = "Signal"

dependencyResolutionManagement {
    versionCatalogs {
        create("lib") {
            from(files("libs.versions.toml"))
        }
    }
}
include("api")
include("signal-mongo")
include("signal-mongo-observation")
include("signal-mongo-benchmark")
