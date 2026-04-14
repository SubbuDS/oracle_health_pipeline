pipeline {
  agent any

  stages {

    stage('Validate') {
      steps {
        sh 'echo "Running validation..."'
        sh 'python3 -c "import sys; sys.path.insert(0, \".\"); print(\"Python OK\")"'
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
