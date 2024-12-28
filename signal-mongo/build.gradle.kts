import net.ltgt.gradle.errorprone.errorprone

plugins {
    id("java")
    id("java-library")
    id("net.ltgt.errorprone") version "4.1.0"
}

group = "signal.mongo"
version = "1.0"

dependencies {
    api(project(":api"))
    implementation(lib.guava)
    implementation(lib.mongodriver)
    implementation(lib.failsafe)
    implementation(lib.slf4j)
    errorprone("com.google.errorprone:error_prone_core:2.28.0")


    runtimeOnly(lib.logback)

    compileOnly(lib.autoserviceannotations)
    compileOnly(lib.autovalueannotations)
    annotationProcessor(lib.autoservice)
    annotationProcessor(lib.autovalue)

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("junit:junit:4.13.2")
    testImplementation(lib.systemrule)
}

tasks.javadoc {
    // 配置 Javadoc 的选项
    options {
        // 启用作者和版本信息
        version = true

        // 设置 Javadoc 的编码方式（如果你的源代码包含非英文字符）
        encoding = "UTF-8"
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.errorprone.disableWarningsInGeneratedCode.set(true)
}