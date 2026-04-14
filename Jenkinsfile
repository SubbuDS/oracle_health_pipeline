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

  }
}
