plugins {
    id("java")
}

group = "signal"
version = "1.0"

allprojects {
    repositories {
        mavenCentral()
    }
}


tasks.test {
    useJUnitPlatform()
}