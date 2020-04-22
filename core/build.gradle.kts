plugins {
    kotlin("jvm") version "1.3.50"
}

dependencies {
    val versions: Map<String, String> by project.extra

    implementation(kotlin("stdlib-jdk8"))
    implementation("org.apache.httpcomponents.core5:httpcore5:${versions["httpcore"]}")
    implementation("org.apache.httpcomponents.core5:httpcore5-h2:${versions["httpcore"]}")
    implementation("com.github.ok2c.hc5:hc5-async-json:${versions["hc5-async-json"]}")
    implementation("org.slf4j:slf4j-api:${versions["slf4j"]}")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${versions["log4j"]}")
    implementation("org.apache.logging.log4j:log4j-core:${versions["log4j"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:${versions["junit-jupiter"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${versions["junit-jupiter"]}")
    testImplementation("org.assertj:assertj-core:${versions["assertj"]}")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
