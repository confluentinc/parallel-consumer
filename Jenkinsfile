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
    def maven_command = sh(script: """if test -f "${env.WORKSPACE}/mvnw"; then echo "${env.WORKSPACE}/mvnw"; else echo "mvn"; fi""", returnStdout: true).trim()
    // If we have a RELEASE_TAG specified as a build parameter, test that the version in pom.xml matches the tag.
    if (!params.RELEASE_TAG.trim().equals('')) {
        sh "git checkout ${params.RELEASE_TAG}"
        def project_version = sh(
                script: '${maven_command} help:evaluate -Dexpression=project.version -q -DforceStdout | tail -1',
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
            def mavenSettingsFile = "${env.WORKSPACE_TMP}/maven-global-settings.xml"             
            withMavenSettings("maven/jenkins_maven_global_settings", "settings", "MAVEN_GLOBAL_SETTINGS", mavenSettingsFile) {
                withMaven(globalMavenSettingsFilePath: mavenSettingsFile) {
                    withDockerServer([uri: dockerHost()]) {
                        def isPrBuild = env.CHANGE_TARGET ? true : false
                        def buildPhase = isPrBuild ? "install" : "deploy"
                        if (params.RELEASE_TAG.trim().equals('')) {
                            sh "${maven_command} --batch-mode -Pjenkins -Pci -U dependency:analyze clean $buildPhase"
                        } else {
                            // it's a parameterized job, and we should deploy to maven central.
                          withGPGkey("gpg/confluent-packaging-private-8B1DA6120C2BF624") {
                            sh "${maven_command} --batch-mode clean deploy -P maven-central -Pjenkins -Pci -Dgpg.passphrase=$GPG_PASSPHRASE"
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
