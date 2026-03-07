# Maven Central Deployment Guide for dataset4j

This guide walks through the process of deploying dataset4j to Maven Central using the **new Central Portal** (OSSRH was sunset on June 30, 2025).

## Prerequisites Completed ✅

Your `pom.xml` is now configured with all required Maven Central elements:
- ✅ Required metadata (name, description, URL, licenses, developers, SCM)
- ✅ Central Publishing Plugin (replaced legacy OSSRH)
- ✅ GPG signing profile
- ✅ Source jar generation

## Next Steps

### 1. Create Central Portal Account

1. **Sign up** at https://central.sonatype.com/
2. **Register your namespace** `io.github.amah`:
   - Go to "Add Namespace" in the portal
   - Enter: `io.github.amah`
   - Verify GitHub ownership when prompted

3. **Verification is automatic** for GitHub namespaces (no JIRA ticket needed)

### 2. Setup GPG Signing

```bash
# Generate GPG key
gpg --gen-key

# List your keys to get the key ID
gpg --list-secret-keys --keyid-format=long

# Upload to key servers
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
gpg --keyserver keys.openpgp.org --send-keys YOUR_KEY_ID
```

### 3. Configure Maven Settings

Create/update `~/.m2/settings.xml`:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
          http://maven.apache.org/xsd/settings-1.0.0.xsd">

    <servers>
        <server>
            <id>central</id>
            <username>your-central-portal-username</username>
            <password>your-central-portal-token</password>
        </server>
    </servers>

    <profiles>
        <profile>
            <id>central-publishing</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
            <properties>
                <gpg.executable>gpg</gpg.executable>
                <gpg.passphrase>your-gpg-passphrase</gpg.passphrase>
            </properties>
        </profile>
    </profiles>
</settings>
```

**Note**: Get your user token from the Central Portal at https://central.sonatype.com/ → Profile → User Token

### 4. Configuration Complete

The POM is already configured with:
- **GroupId**: `io.github.amah`
- **GitHub URLs**: All pointing to `https://github.com/amah/dataset4j`
- **Developer info**: Set up with your details

No additional setup required - ready for deployment!

### 5. Release Process

#### Step 1: Prepare Release
```bash
# Change version to release version
mvn versions:set -DnewVersion=1.0.0
mvn versions:commit

# Commit and tag
git add .
git commit -m "Release 1.0.0"
git tag v1.0.0
```

#### Step 2: Deploy to Central Portal
```bash
# Deploy with release profile (includes GPG signing and auto-publish)
mvn clean deploy -P release
```

The new Central Portal will:
1. **Validate** your artifacts automatically
2. **Sign** with GPG during build
3. **Auto-publish** to Maven Central (due to `<autoPublish>true</autoPublish>`)

#### Step 3: Monitor Deployment
1. Check the deployment status at https://central.sonatype.com/
2. Go to "Deployments" to see progress
3. **No manual release step needed** - it's automatic!

#### Step 4: Post-Release Cleanup
```bash
# Push to GitHub
git push origin main --tags

# Bump to next development version
mvn versions:set -DnewVersion=1.1.0-SNAPSHOT
mvn versions:commit
git add .
git commit -m "Bump to 1.1.0-SNAPSHOT"
git push origin main
```

### 6. Verification

After 2-4 hours, verify your artifact is available:

```xml
<dependency>
    <groupId>io.github.amah</groupId>
    <artifactId>dataset4j</artifactId>
    <version>1.0.0</version>
</dependency>
```

Search for it on:
- https://search.maven.org/
- https://mvnrepository.com/

## Common Issues

1. **GPG signing fails**: Ensure GPG is installed and key is generated
2. **Validation errors**: Check that all required metadata is present
3. **Upload timeout**: Large artifacts may take time, be patient
4. **Permission denied**: Ensure your Sonatype account has rights to your groupId

## Support

- Sonatype OSSRH Guide: https://central.sonatype.org/publish/publish-guide/
- Maven GPG Plugin: https://maven.apache.org/plugins/maven-gpg-plugin/
- Staging Repository Guide: https://central.sonatype.org/publish/release/

Your dataset4j library is now ready for Maven Central deployment! 🚀