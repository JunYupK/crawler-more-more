#!/usr/bin/env python3
"""
Unit Tests for CI/CD Pipeline

각 컴포넌트의 단위 기능을 테스트합니다.
pytest와 함께 사용 가능합니다.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestConfiguration:
    """설정 파일 테스트"""

    def test_aggressive_settings_import(self):
        """공격적 최적화 설정 import 테스트"""
        from config import settings
        assert settings is not None

    def test_semaphore_limit(self):
        """Semaphore 한계 설정 테스트"""
        from config.settings import GLOBAL_SEMAPHORE_LIMIT
        assert GLOBAL_SEMAPHORE_LIMIT == 200, "Semaphore limit should be 200"

    def test_tcp_connector_limit(self):
        """TCP Connector 한계 설정 테스트"""
        from config.settings import TCP_CONNECTOR_LIMIT
        assert TCP_CONNECTOR_LIMIT == 300, "TCP connector limit should be 300"

    def test_worker_threads(self):
        """워커 스레드 수 테스트"""
        from config.settings import WORKER_THREADS
        assert WORKER_THREADS == 16, "Worker threads should be 16"

    def test_batch_size(self):
        """배치 크기 테스트"""
        from config.settings import BATCH_SIZE
        assert BATCH_SIZE == 100, "Batch size should be 100"


class TestCrawlerModules:
    """크롤러 모듈 테스트"""

    def test_polite_crawler_import(self):
        """PoliteCrawler import 테스트"""
        import polite_crawler
        assert hasattr(polite_crawler, 'PoliteCrawler')

    def test_multithreaded_crawler_import(self):
        """MultithreadedCrawler import 테스트"""
        import multithreaded_crawler
        assert hasattr(multithreaded_crawler, 'MultithreadedCrawler')

    def test_distributed_crawler_import(self):
        """DistributedCrawler import 테스트"""
        import distributed_crawler
        assert hasattr(distributed_crawler, 'DistributedCrawlerWorker')

    def test_sharded_queue_manager_import(self):
        """ShardedQueueManager import 테스트"""
        import sharded_queue_manager
        assert hasattr(sharded_queue_manager, 'ShardedQueueManager')


class TestMonitoring:
    """모니터링 모듈 테스트"""

    def test_metrics_import(self):
        """Metrics 모듈 import 테스트"""
        from monitoring import metrics
        assert metrics is not None

    def test_metrics_collector(self):
        """MetricsCollector 클래스 테스트"""
        from monitoring.metrics import MetricsCollector
        collector = MetricsCollector()
        assert collector is not None


class TestDatabase:
    """데이터베이스 모듈 테스트"""

    def test_database_import(self):
        """Database 모듈 import 테스트"""
        import database
        assert database is not None


class TestKubernetesManifests:
    """Kubernetes 매니페스트 테스트"""

    def test_namespace_manifest_exists(self):
        """Namespace 매니페스트 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'base' / 'namespace.yaml'
        assert manifest_path.exists(), "Namespace manifest should exist"

    def test_configmap_manifest_exists(self):
        """ConfigMap 매니페스트 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'base' / 'configmap.yaml'
        assert manifest_path.exists(), "ConfigMap manifest should exist"

    def test_deployment_manifest_exists(self):
        """Deployment 매니페스트 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'base' / 'crawler-deployment.yaml'
        assert manifest_path.exists(), "Deployment manifest should exist"

    def test_keda_manifest_exists(self):
        """KEDA ScaledObject 매니페스트 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'autoscaling' / 'keda-scaledobject.yaml'
        assert manifest_path.exists(), "KEDA manifest should exist"


class TestMonitoringStack:
    """모니터링 스택 테스트"""

    def test_prometheus_configmap_exists(self):
        """Prometheus ConfigMap 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'monitoring' / 'prometheus' / 'prometheus-configmap.yaml'
        assert manifest_path.exists(), "Prometheus ConfigMap should exist"

    def test_grafana_deployment_exists(self):
        """Grafana Deployment 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'monitoring' / 'grafana' / 'grafana-deployment.yaml'
        assert manifest_path.exists(), "Grafana Deployment should exist"

    def test_dashboard_json_exists(self):
        """Grafana Dashboard JSON 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'monitoring' / 'grafana' / 'crawler-dashboard.json'
        assert manifest_path.exists(), "Dashboard JSON should exist"

    def test_exporters_manifest_exists(self):
        """Exporters 매니페스트 존재 확인"""
        manifest_path = Path(__file__).parent.parent / 'k8s' / 'monitoring' / 'exporters.yaml'
        assert manifest_path.exists(), "Exporters manifest should exist"


class TestCICDWorkflows:
    """CI/CD Workflow 테스트"""

    def test_ci_workflow_exists(self):
        """CI workflow 존재 확인"""
        workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'ci.yml'
        assert workflow_path.exists(), "CI workflow should exist"

    def test_docker_build_workflow_exists(self):
        """Docker build workflow 존재 확인"""
        workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'docker-build.yml'
        assert workflow_path.exists(), "Docker build workflow should exist"

    def test_deploy_workflow_exists(self):
        """Deploy workflow 존재 확인"""
        workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'deploy-k8s.yml'
        assert workflow_path.exists(), "Deploy workflow should exist"

    def test_pr_automation_workflow_exists(self):
        """PR automation workflow 존재 확인"""
        workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'pr-automation.yml'
        assert workflow_path.exists(), "PR automation workflow should exist"

    def test_release_workflow_exists(self):
        """Release workflow 존재 확인"""
        workflow_path = Path(__file__).parent.parent.parent / '.github' / 'workflows' / 'release.yml'
        assert workflow_path.exists(), "Release workflow should exist"

    def test_cicd_guide_exists(self):
        """CI/CD 가이드 문서 존재 확인"""
        guide_path = Path(__file__).parent.parent.parent / '.github' / 'CI_CD.md'
        assert guide_path.exists(), "CI/CD guide should exist"


class TestDockerfile:
    """Dockerfile 테스트"""

    def test_dockerfile_exists(self):
        """Dockerfile 존재 확인"""
        dockerfile_path = Path(__file__).parent.parent / 'Dockerfile'
        assert dockerfile_path.exists(), "Dockerfile should exist"

    def test_dockerfile_has_multistage(self):
        """Dockerfile이 multi-stage build를 사용하는지 확인"""
        dockerfile_path = Path(__file__).parent.parent / 'Dockerfile'
        with open(dockerfile_path, 'r') as f:
            content = f.read()
        assert 'FROM' in content, "Dockerfile should have FROM instruction"
        assert content.count('FROM') >= 2, "Dockerfile should use multi-stage build"

    def test_dockerfile_has_nonroot_user(self):
        """Dockerfile이 non-root user를 사용하는지 확인"""
        dockerfile_path = Path(__file__).parent.parent / 'Dockerfile'
        with open(dockerfile_path, 'r') as f:
            content = f.read()
        assert 'USER' in content, "Dockerfile should specify USER"
        assert 'crawler' in content, "Dockerfile should use crawler user"


class TestRequirements:
    """의존성 패키지 테스트"""

    def test_requirements_file_exists(self):
        """requirements.txt 존재 확인"""
        req_path = Path(__file__).parent.parent / 'requirements.txt'
        assert req_path.exists(), "requirements.txt should exist"

    def test_essential_packages(self):
        """필수 패키지 확인"""
        req_path = Path(__file__).parent.parent / 'requirements.txt'
        with open(req_path, 'r') as f:
            requirements = f.read()

        essential_packages = [
            'aiohttp',
            'beautifulsoup4',
            'redis',
            'psycopg2-binary'
        ]

        for package in essential_packages:
            assert package in requirements, f"{package} should be in requirements.txt"


if __name__ == '__main__':
    # Run tests with pytest if available, otherwise run with unittest
    try:
        import pytest
        pytest.main([__file__, '-v'])
    except ImportError:
        import unittest
        unittest.main()
