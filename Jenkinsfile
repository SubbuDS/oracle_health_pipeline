pipeline {
  agent any

  stages {

    stage('Validate') {
      steps {
        sh 'echo "Running validation..."'
        sh 'python3 -c "import jobs.bronze_streaming; print(\"Import OK\")"'
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
