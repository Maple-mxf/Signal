plugins {
    id("java")
}

group = "signal.api"
version = "1.0"

dependencies {
    implementation(lib.guava)
    implementation(lib.failsafe)

    compileOnly(lib.autovalueannotations)
    compileOnly(lib.autoserviceannotations)

    annotationProcessor(lib.autovalue)
    annotationProcessor(lib.autoservice)
}

sourceSets {
    main {
        java {
            srcDirs("src/main/java", "build/generated/sources/annotationProcessor/java/main")
        }
    }
}

tasks.withType<JavaCompile> {
    options.annotationProcessorPath = configurations["annotationProcessor"]
}