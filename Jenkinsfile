#!/usr/bin/env groovy

//common {
//  slackChannel = 'csid-build'
//  nodeLabel = 'docker-openjdk13'
//  runMergeCheck = false
//}

def RelaseTag = string(name: 'RELEASE_TAG', defaultValue: '',
        description: 'Provide the tag of project that will be release to maven central,' +
                'only use the value when you want to release to maven central')

def config = jobConfig {
    owner = 'csid'
//  testResultSpecs = ['junit': 'test/results.xml']
    properties = [parameters([RelaseTag])]
    slackChannel = 'csid-build'
    nodeLabel = 'docker-debian-jdk17'
    runMergeCheck = true
}

def job = {
    // If we have a RELEASE_TAG specified as a build parameter, test that the version in pom.xml matches the tag.
    if (!params.RELEASE_TAG.trim().equals('')) {
        sh "git checkout ${params.RELEASE_TAG}"
        def project_version = sh(
                script: 'mvn help:evaluate -Dexpression=project.version -q -DforceStdout | tail -1',
                returnStdout: true
        ).trim()

        if (!params.RELEASE_TAG.trim().equals(project_version)) {
            echo 'ERROR: tag doesn\'t match project version, please correct and try again'
            echo "Tag: ${params.RELEASE_TAG}"
            echo "Project version: ${project_version}"
            currentBuild.result = 'FAILURE'
            return
        }
    }

    stage('Build') {
        archiveArtifacts artifacts: 'pom.xml'
        withVaultEnv([["gpg/confluent-packaging-private-8B1DA6120C2BF624", "passphrase", "GPG_PASSPHRASE"]]) {
            withVaultFile([["maven/jenkins_maven_global_settings", "settings_xml", "maven-global-settings.xml", "MAVEN_GLOBAL_SETTINGS_FILE"]]) {
                withMaven(globalMavenSettingsFilePath: "${env.MAVEN_GLOBAL_SETTINGS_FILE}") {
                    withDockerServer([uri: dockerHost()]) {
                        def isPrBuild = env.CHANGE_TARGET ? true : false
                        def buildPhase = isPrBuild ? "install" : "deploy"
                        pip install twine
                        if (params.RELEASE_TAG.trim().equals('')) {
                            sh "mvn --batch-mode -Pjenkins -Pci -U dependency:analyze clean $buildPhase"

                            withVaultEnv([["pypi/test.pypi.org", "user", "TWINE_USERNAME"],
                                          ["pypi/test.pypi.org", "password", "TWINE_PASSWORD"]]) {
                              twine upload --repository-url https://test.pypi.org/legacy/ ./parallel-consumer-python/dist/* --verbose --non-interactive
                            }
                        } else {
                            // it's a parameterized job, and we should deploy to maven central.
                          withGPGkey("gpg/confluent-packaging-private-8B1DA6120C2BF624") {
                            sh "mvn --batch-mode clean deploy -P maven-central -Pjenkins -Pci -Dgpg.passphrase=$GPG_PASSPHRASE"
                          }
                          withVaultEnv([["pypi/pypi.org", "user", "TWINE_USERNAME"],
                                        ["pypi/pypi.org", "password", "TWINE_PASSWORD"]]) {
                            twine upload --repository-url https://upload.pypi.org/legacy/ ./parallel-consumer-python/dist/* --verbose --non-interactive
                          }
                        }
                        currentBuild.result = 'Success'
                    }
                }
            }
        }

    }
}
runJob config, job
