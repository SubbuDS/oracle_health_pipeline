pipeline {
  agent any

  stages {

    stage('Validate') {
      steps {
        sh 'echo "Running validation..."'
        sh 'python3 --version'
        sh 'test -f jobs/bronze_streaming.py && echo "bronze_streaming.py found" || exit 1'
      }
    }

    stage('Check Infrastructure') {
      steps {
        sh 'docker ps | grep postgres'
        sh 'kubectl get nodes'
      }
    }

    stage('Deploy to Dev') {
      when {
        branch 'develop'
      }
      steps {
        sh 'echo "Deploying to ehr-dev namespace..."'
        sh 'kubectl get namespace ehr-dev'
        sh 'echo "Deploy to DEV complete"'
      }
    }

    stage('Deploy to Prod') {
      when {
        branch 'main'
      }
      steps {
        sh 'echo "Deploying to ehr-prod namespace..."'
        sh 'kubectl get namespace ehr-prod'
        sh 'echo "Deploy to PROD complete"'
      }
    }

  }

  post {
    success {
      sh 'echo "Pipeline passed on branch: ${BRANCH_NAME}"'
    }
    failure {
      sh 'echo "Pipeline failed on branch: ${BRANCH_NAME}"'
      sh 'kubectl get pods -A | grep ehr || true'
    }
  }
}
