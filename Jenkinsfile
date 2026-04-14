pipeline {
  agent any

  stages {

    stage('Hello') {
      steps {
        sh 'echo "Jenkins is alive"'
      }
    }

    stage('Check Docker') {
      steps {
        sh 'docker ps'
      }
    }

    stage('Check Kubectl') {
      steps {
        sh 'kubectl get nodes'
      }
    }

  }
}
