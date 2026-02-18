plugins {
    java
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    compileOnly("jakarta.validation:jakarta.validation-api:3.1.0")
}
