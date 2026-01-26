plugins {
    `java-library`
    // [修正] 使用新的 Plugin ID 和最新版本
    id("io.papermc.paperweight.userdev") version "2.0.0-beta.19"
    // 快速启动服务端测试插件
    id("xyz.jpenilla.run-paper") version "2.3.0"
    // Shadow 插件
    id("com.gradleup.shadow") version "9.3.1"
}

group = "top.ellan"
version = "1.0-SNAPSHOT"

java {
    // 保持使用 Java 25 (开启 Vector API 必须)
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
}

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
    maven("https://repo.extendedclip.com/content/repositories/placeholderapi/")
    maven("https://repo.nightexpressdev.com/releases")
    maven("https://jitpack.io")
    maven("https://repo.lanink.cn/repository/maven-public/")
    
    flatDir { dirs("libs") }
}

dependencies {
    // 1. 核心开发环境
    paperweight.paperDevBundle("1.21.11-R0.1-SNAPSHOT")

    // 2. 外部插件依赖
    compileOnly("me.clip:placeholderapi:2.11.6")
    compileOnly("su.nightexpress.coinsengine:CoinsEngine:2.6.0")
    compileOnly("su.nightexpress.nightcore:main:2.13.0")
    compileOnly("cn.superiormc.ultimateshop:plugin:4.2.3")

    // 本地 libs
    compileOnly(fileTree(mapOf("dir" to "libs", "include" to listOf("*.jar"))))

    // 3. 知识提取/数据处理库
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.17.0"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-annotations")

    implementation("redis.clients:jedis:5.2.0")
    implementation("com.zaxxer:HikariCP:7.0.2")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.3")
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
        // [必要修改] 必须设置为 25 以匹配 toolchain 并支持 Vector API
        options.release.set(25)
        // [必要修改] 开启预览特性和 Vector 孵化模块
        options.compilerArgs.addAll(listOf(
            "--enable-preview",
            "--add-modules=jdk.incubator.vector"
        ))
    }

    processResources {
        val props = mapOf("version" to version)
        inputs.properties(props)
        filteringCharset = "UTF-8"
        filesMatching("plugin.yml") {
            expand(props)
        }
    }

    // ShadowJar 配置
    named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        archiveClassifier.set("")
        val prefix = "top.ellan.ecobridge.libs"

        relocate("com.fasterxml.jackson", "$prefix.jackson")
        relocate("com.zaxxer.hikari", "$prefix.hikari")
        relocate("redis.clients", "$prefix.jedis")
        relocate("org.apache.commons.pool2", "$prefix.commons.pool2")
        relocate("org.json", "$prefix.json")
        relocate("com.github.benmanes.caffeine", "$prefix.caffeine")

        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
        exclude("META-INF/maven/**")
        
        minimize {
            exclude(dependency("com.zaxxer:HikariCP:.*"))
        }
    }
    
    build {
        dependsOn("shadowJar")
    }
}