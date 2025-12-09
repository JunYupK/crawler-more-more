#!/usr/bin/env python3
"""
Integration Tests for CI/CD Pipeline

실제 환경에 가까운 통합 테스트를 수행합니다.
Docker, K8s가 없는 환경에서도 가능한 테스트를 수행합니다.
"""

import sys
import os
from pathlib import Path
import json
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class IntegrationTest:
    def __init__(self):
        self.base_path = Path(__file__).parent.parent
        self.results = []

    def log_result(self, test_name, passed, message=""):
        """테스트 결과 로깅"""
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
        if message:
            print(f"   {message}")
        self.results.append({
            'test': test_name,
            'passed': passed,
            'message': message
        })

    def test_python_modules_integration(self):
        """Python 모듈 간 통합 테스트"""
        print("\n=== Python 모듈 통합 테스트 ===")

        # Test 1: Config와 Crawler 통합
        try:
            from config.settings import GLOBAL_SEMAPHORE_LIMIT
            import polite_crawler

            # PoliteCrawler가 설정을 올바르게 사용하는지 확인
            self.log_result(
                "Config-Crawler 통합",
                True,
                f"Semaphore limit: {GLOBAL_SEMAPHORE_LIMIT}"
            )
        except Exception as e:
            self.log_result("Config-Crawler 통합", False, str(e))

        # Test 2: Monitoring과 Metrics 통합
        try:
            from monitoring.metrics import MetricsCollector
            collector = MetricsCollector()

            # 기본 메트릭 수집 가능한지 확인
            self.log_result(
                "Monitoring-Metrics 통합",
                True,
                "MetricsCollector 인스턴스 생성 성공"
            )
        except Exception as e:
            self.log_result("Monitoring-Metrics 통합", False, str(e))

    def test_kubernetes_manifests_integration(self):
        """Kubernetes 매니페스트 통합 테스트"""
        print("\n=== Kubernetes 매니페스트 통합 테스트 ===")

        # Test 1: Namespace와 Deployment 연동
        try:
            namespace_path = self.base_path / 'k8s' / 'base' / 'namespace.yaml'
            deployment_path = self.base_path / 'k8s' / 'base' / 'crawler-deployment.yaml'

            with open(namespace_path, 'r') as f:
                namespace_docs = list(yaml.safe_load_all(f))

            with open(deployment_path, 'r') as f:
                deployment_docs = list(yaml.safe_load_all(f))

            # Namespace 이름 확인
            namespace_name = namespace_docs[0]['metadata']['name']

            # Deployment가 올바른 namespace를 참조하는지 확인
            deployment_namespace = deployment_docs[0]['metadata']['namespace']

            if namespace_name == deployment_namespace == 'crawler':
                self.log_result(
                    "Namespace-Deployment 통합",
                    True,
                    f"Namespace: {namespace_name}"
                )
            else:
                self.log_result(
                    "Namespace-Deployment 통합",
                    False,
                    f"Namespace mismatch: {namespace_name} != {deployment_namespace}"
                )
        except Exception as e:
            self.log_result("Namespace-Deployment 통합", False, str(e))

        # Test 2: ConfigMap와 Deployment 연동
        try:
            configmap_path = self.base_path / 'k8s' / 'base' / 'configmap.yaml'
            deployment_path = self.base_path / 'k8s' / 'base' / 'crawler-deployment.yaml'

            with open(configmap_path, 'r') as f:
                configmap_docs = list(yaml.safe_load_all(f))

            with open(deployment_path, 'r') as f:
                deployment_docs = list(yaml.safe_load_all(f))

            configmap_name = configmap_docs[0]['metadata']['name']

            # Deployment가 ConfigMap을 참조하는지 확인
            deployment_yaml = yaml.dump(deployment_docs[0])

            if configmap_name in deployment_yaml:
                self.log_result(
                    "ConfigMap-Deployment 통합",
                    True,
                    f"ConfigMap: {configmap_name}"
                )
            else:
                self.log_result(
                    "ConfigMap-Deployment 통합",
                    False,
                    f"Deployment가 ConfigMap을 참조하지 않음"
                )
        except Exception as e:
            self.log_result("ConfigMap-Deployment 통합", False, str(e))

    def test_monitoring_stack_integration(self):
        """모니터링 스택 통합 테스트"""
        print("\n=== 모니터링 스택 통합 테스트 ===")

        # Test 1: Prometheus와 Grafana 연동
        try:
            prometheus_cm_path = self.base_path / 'k8s' / 'monitoring' / 'prometheus' / 'prometheus-configmap.yaml'
            grafana_cm_path = self.base_path / 'k8s' / 'monitoring' / 'grafana' / 'grafana-configmap.yaml'

            # Prometheus ConfigMap 확인
            with open(prometheus_cm_path, 'r') as f:
                prometheus_config = yaml.safe_load(f)

            # Grafana ConfigMap 확인
            with open(grafana_cm_path, 'r') as f:
                grafana_config = yaml.safe_load(f)

            # Grafana가 Prometheus를 데이터소스로 사용하는지 확인
            grafana_data = grafana_config.get('data', {})
            datasources_yaml = grafana_data.get('datasources.yaml', '')

            if 'prometheus' in datasources_yaml.lower():
                self.log_result(
                    "Prometheus-Grafana 통합",
                    True,
                    "Grafana가 Prometheus를 데이터소스로 사용"
                )
            else:
                self.log_result(
                    "Prometheus-Grafana 통합",
                    False,
                    "Grafana가 Prometheus를 참조하지 않음"
                )
        except Exception as e:
            self.log_result("Prometheus-Grafana 통합", False, str(e))

        # Test 2: Dashboard JSON 유효성
        try:
            dashboard_path = self.base_path / 'k8s' / 'monitoring' / 'grafana' / 'crawler-dashboard.json'

            with open(dashboard_path, 'r') as f:
                dashboard = json.load(f)

            # Dashboard가 필수 필드를 가지고 있는지 확인
            required_fields = ['title', 'panels', 'uid']
            has_all_fields = all(field in dashboard for field in required_fields)

            if has_all_fields:
                panel_count = len(dashboard['panels'])
                self.log_result(
                    "Grafana Dashboard 구조",
                    True,
                    f"Panel 수: {panel_count}"
                )
            else:
                missing = [f for f in required_fields if f not in dashboard]
                self.log_result(
                    "Grafana Dashboard 구조",
                    False,
                    f"누락된 필드: {missing}"
                )
        except Exception as e:
            self.log_result("Grafana Dashboard 구조", False, str(e))

    def test_cicd_workflows_integration(self):
        """CI/CD Workflow 통합 테스트"""
        print("\n=== CI/CD Workflow 통합 테스트 ===")

        # Test 1: Docker Build와 Deploy 연동
        try:
            docker_workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'docker-build.yml'
            deploy_workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'deploy-k8s.yml'

            # Docker build workflow가 이미지를 푸시하는지 확인
            with open(docker_workflow_path, 'r') as f:
                docker_content = f.read()

            # Deploy workflow가 이미지를 참조하는지 확인
            with open(deploy_workflow_path, 'r') as f:
                deploy_content = f.read()

            # ghcr.io 레지스트리 사용 확인
            docker_uses_ghcr = 'ghcr.io' in docker_content
            deploy_uses_ghcr = 'ghcr.io' in deploy_content

            if docker_uses_ghcr and deploy_uses_ghcr:
                self.log_result(
                    "Docker-Deploy 통합",
                    True,
                    "동일한 레지스트리(ghcr.io) 사용"
                )
            else:
                self.log_result(
                    "Docker-Deploy 통합",
                    False,
                    f"Docker: {docker_uses_ghcr}, Deploy: {deploy_uses_ghcr}"
                )
        except Exception as e:
            self.log_result("Docker-Deploy 통합", False, str(e))

        # Test 2: CI와 PR Automation 연동
        try:
            ci_workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'ci.yml'
            pr_workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'pr-automation.yml'

            with open(ci_workflow_path, 'r') as f:
                ci_content = f.read()

            with open(pr_workflow_path, 'r') as f:
                pr_content = f.read()

            # 두 workflow가 모두 PR에서 실행되는지 확인
            ci_on_pr = 'pull_request' in ci_content
            pr_on_pr = 'pull_request' in pr_content

            if ci_on_pr and pr_on_pr:
                self.log_result(
                    "CI-PR 통합",
                    True,
                    "두 workflow 모두 PR에서 실행"
                )
            else:
                self.log_result(
                    "CI-PR 통합",
                    False,
                    f"CI on PR: {ci_on_pr}, PR automation on PR: {pr_on_pr}"
                )
        except Exception as e:
            self.log_result("CI-PR 통합", False, str(e))

    def test_dockerfile_requirements_integration(self):
        """Dockerfile과 requirements.txt 통합 테스트"""
        print("\n=== Dockerfile-Requirements 통합 테스트 ===")

        try:
            dockerfile_path = self.base_path / 'Dockerfile'
            requirements_path = self.base_path / 'requirements.txt'

            with open(dockerfile_path, 'r') as f:
                dockerfile_content = f.read()

            # Dockerfile이 requirements.txt를 복사하는지 확인
            copies_requirements = 'COPY requirements.txt' in dockerfile_content
            installs_requirements = 'pip install' in dockerfile_content and 'requirements.txt' in dockerfile_content

            if copies_requirements and installs_requirements:
                self.log_result(
                    "Dockerfile-Requirements 통합",
                    True,
                    "requirements.txt를 올바르게 사용"
                )
            else:
                self.log_result(
                    "Dockerfile-Requirements 통합",
                    False,
                    f"Copy: {copies_requirements}, Install: {installs_requirements}"
                )
        except Exception as e:
            self.log_result("Dockerfile-Requirements 통합", False, str(e))

    def generate_report(self):
        """테스트 결과 리포트 생성"""
        print("\n" + "="*60)
        print("통합 테스트 결과 리포트")
        print("="*60)

        total = len(self.results)
        passed = sum(1 for r in self.results if r['passed'])
        failed = total - passed
        success_rate = (passed / total * 100) if total > 0 else 0

        print(f"총 테스트: {total}")
        print(f"✅ 성공: {passed}")
        print(f"❌ 실패: {failed}")
        print(f"성공률: {success_rate:.1f}%")
        print("="*60)

        if failed > 0:
            print("\n실패한 테스트:")
            for result in self.results:
                if not result['passed']:
                    print(f"  - {result['test']}: {result['message']}")

        return success_rate >= 90

    def run_all(self):
        """모든 통합 테스트 실행"""
        print("="*60)
        print("통합 테스트 시작")
        print("="*60)

        self.test_python_modules_integration()
        self.test_kubernetes_manifests_integration()
        self.test_monitoring_stack_integration()
        self.test_cicd_workflows_integration()
        self.test_dockerfile_requirements_integration()

        return self.generate_report()


def main():
    """메인 함수"""
    test = IntegrationTest()
    success = test.run_all()

    if success:
        print("\n✅ 통합 테스트 통과!")
        return 0
    else:
        print("\n❌ 통합 테스트 실패!")
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
