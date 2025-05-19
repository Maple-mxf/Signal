rootProject.name = "Signal"

dependencyResolutionManagement {
    versionCatalogs {
        create("lib") {
            from(files("libs.versions.toml"))
        }
    }
}
include("api")
include("core")
include("observation")
include("benchmark")
