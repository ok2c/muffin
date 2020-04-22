val versions = mapOf(
    "httpcore"         to "5.1-beta3",
    "hc5-async-json"   to "0.2.1",
    "slf4j"            to "1.7.25",
    "log4j"            to "2.8.2",
    "junit-jupiter"    to "5.6.0",
    "mockito"          to "2.28.2",
    "assertj"          to "3.15.0"
)

allprojects {
    group = "com.ok2c.muffin"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    extra["versions"] = versions
}
