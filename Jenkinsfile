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
    nodeLabel = 'docker-openjdk13'
    runMergeCheck = false
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
            withVaultFile([["maven/jenkins_maven_global_settings", "settings_xml", "maven-global-settings.xml", "MAVEN_GLOBAL_SETTINGS_FILE"],
                           ["gpg/confluent-packaging-private-8B1DA6120C2BF624", "private_key", "confluent-packaging-private.key", "GPG_PRIVATE_KEY"]]) {
                withMaven(globalMavenSettingsFilePath: "${env.MAVEN_GLOBAL_SETTINGS_FILE}") {
                    withDockerServer([uri: dockerHost()]) {
                        if (params.RELEASE_TAG.trim().equals('')) {
                            sh "mvn --batch-mode -Pjenkins -Pci clean verify install dependency:analyze site validate -U"
                        } else {
                            // it's a parameterized job, and we should deploy to maven central.
                            sh '''
                          set +x
                          gpg --import < $GPG_PRIVATE_KEY;
                          mvn --batch-mode clean deploy -P maven-central -Pjenkins -Pci -Dgpg.passphrase=$GPG_PASSPHRASE
                      '''
                        }
                        currentBuild.result = 'Success'
                    }
                }
            }
        }
    }
}
runJob config, job
