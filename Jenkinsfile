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

    stage('Terraform Dev') {
      when { branch 'develop' }
      steps {
        dir('terraform') {
          sh 'terraform init -input=false'
          sh 'terraform apply -input=false -auto-approve -var="environment=dev"'
        }
      }
    }

    stage('Terraform Prod') {
      when { branch 'main' }
      steps {
        dir('terraform') {
          sh 'terraform init -input=false'
          sh 'terraform apply -input=false -auto-approve -var="environment=prod"'
        }
      }
    }

    stage('Deploy to Dev') {
      when { branch 'develop' }
      steps {
        sh 'echo "Verifying ehr-dev resources..."'
        sh 'kubectl get namespace ehr-dev'
        sh 'docker exec postgres psql -U demo -d ehr_db -c "\\dt ehr_dev.*"'
        sh 'docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep ehr.dev'
        sh 'echo "DEV deployment complete"'
      }
    }

    stage('Deploy to Prod') {
      when { branch 'main' }
      steps {
        sh 'echo "Verifying ehr-prod resources..."'
        sh 'kubectl get namespace ehr-prod'
        sh 'docker exec postgres psql -U demo -d ehr_db -c "\\dt ehr_prod.*"'
        sh 'docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep ehr.prod'
        sh 'echo "PROD deployment complete"'
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
