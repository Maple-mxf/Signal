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

tasks.javadoc {
    // 配置 Javadoc 的选项
    options {
        // 启用作者和版本信息
        version = true

        // 设置 Javadoc 的编码方式（如果你的源代码包含非英文字符）
        encoding = "UTF-8"
    }
}