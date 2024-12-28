plugins {
    id("java")
    id("java-library")
}

group = "signal.mongo.observation"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":signal-mongo"))

    implementation(lib.bytebuddy)
    implementation(lib.bytebuddyagent)

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}