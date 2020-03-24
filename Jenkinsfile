#!/usr/bin/env groovy

podTemplate(containers: [
    containerTemplate(name: 'cct-airflow', image: 'cityofcapetown/airflow', command: "cat", ttyEnabled: true, livenessProbe: containerLivenessProbe(initialDelaySeconds: 120)),
    containerTemplate(name: 'cct-datascience-python', image: 'cityofcapetown/datascience:python', ttyEnabled: true, command: 'cat')
  ]) {
    node(POD_LABEL) {
        stage('covid-19-data build') {
            git credentialsId: 'jenkins-user', url: 'https://ds1.capetown.gov.za/ds_gitlab/opm/covid-19-data.git'
            container('cct-datascience-python') {
                    sh label: 'package_script', script: '''#!/usr/bin/env bash
                        file=covid-19-data.zip
                        zip -r $file ./*.py ./*.R ./bin
                        exit $?'''
            }
            updateGitlabCommitStatus name: 'build', state: 'success'
        }
        stage('covid-19-data airflow validate') {
            git credentialsId: 'jenkins-user', url: 'https://ds1.capetown.gov.za/ds_gitlab/opm/covid-19-data.git'

            container('cct-airflow') {
                sh '''#!/usr/bin/env bash
                  set -e
                  ./tests/test-dags.sh
                  '''
            }
            updateGitlabCommitStatus name: 'dags-validate', state: 'success'
        }
        stage('covid-19-data dags upload') {
            container('cct-airflow') {
                withCredentials([usernamePassword(credentialsId: 'minio-edge-credentials', passwordVariable: 'MINIO_SECRET', usernameVariable: 'MINIO_ACCESS')]) {
                    sh label: 'dags_upload_script', script: '''#!/usr/bin/env bash
                        set -e
                        cd dags
                        for file in $(ls *.py); do
                            bucket=opm-dags
                            resource="/${bucket}/${file}"
                            contentType="application/octet-stream"
                            dateValue=`date -R`
                            stringToSign="PUT\\n\\n${contentType}\\n${dateValue}\\n${resource}"
                            signature=$(echo -en ${stringToSign} | openssl sha1 -hmac ${MINIO_SECRET} -binary | base64)
                            curl -v --fail \\
                              -X PUT -T "${file}" \\
                              -H "Host: ds2.capetown.gov.za" \\
                              -H "Date: ${dateValue}" \\
                              -H "Content-Type: ${contentType}" \\
                              -H "Authorization: AWS ${MINIO_ACCESS}:${signature}" \\
                              https://ds2.capetown.gov.za/${resource}
                        done
                        exit $?'''
                }
            }
            updateGitlabCommitStatus name: 'upload', state: 'success'
        }
        stage('covid-19-data code upload') {
            container('cct-airflow') {
                withCredentials([usernamePassword(credentialsId: 'minio-edge-credentials', passwordVariable: 'MINIO_SECRET', usernameVariable: 'MINIO_ACCESS')]) {
                    sh label: 'upload_script', script: '''#!/usr/bin/env bash
                        file=covid-19-data.zip
                        bucket=covid-19-data-deploy
                        resource="/${bucket}/${file}"
                        contentType="application/octet-stream"
                        dateValue=`date -R`
                        stringToSign="PUT\\n\\n${contentType}\\n${dateValue}\\n${resource}"
                        signature=$(echo -en ${stringToSign} | openssl sha1 -hmac ${MINIO_SECRET} -binary | base64)
                        curl -v --fail \\
                          -X PUT -T "${file}" \\
                          -H "Host: ds2.capetown.gov.za" \\
                          -H "Date: ${dateValue}" \\
                          -H "Content-Type: ${contentType}" \\
                          -H "Authorization: AWS ${MINIO_ACCESS}:${signature}" \\
                          https://ds2.capetown.gov.za/${resource}
                        exit $?'''
                }
            }
            updateGitlabCommitStatus name: 'upload', state: 'success'
        }
    }
}

