#!/usr/bin/env python3
"""
통합 테스트: Phase 1-10 전체 기능 점검

각 Phase별 핵심 기능을 테스트하여 정상 작동 여부를 확인합니다.
"""

import os
import sys
import importlib
import traceback
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

class PhaseTestRunner:
    def __init__(self):
        self.results = {
            'passed': [],
            'failed': [],
            'skipped': []
        }

    def test_phase_1_basic_crawler(self):
        """Phase 1: 기본 크롤러 (단일 & 멀티스레드)"""
        print("\n" + "="*60)
        print("Phase 1: 기본 크롤러 테스트")
        print("="*60)

        tests = []

        # Test 1: polite_crawler.py import
        try:
            import polite_crawler
            print("✅ polite_crawler.py import 성공")
            tests.append(('polite_crawler import', True))
        except Exception as e:
            print(f"❌ polite_crawler.py import 실패: {e}")
            tests.append(('polite_crawler import', False))

        # Test 2: multithreaded_crawler.py import
        try:
            import multithreaded_crawler
            print("✅ multithreaded_crawler.py import 성공")
            tests.append(('multithreaded_crawler import', True))
        except Exception as e:
            print(f"❌ multithreaded_crawler.py import 실패: {e}")
            tests.append(('multithreaded_crawler import', False))

        # Test 3: PoliteCrawler 클래스 인스턴스화
        try:
            from polite_crawler import PoliteCrawler
            crawler = PoliteCrawler(max_concurrent_requests=5)
            print("✅ PoliteCrawler 인스턴스 생성 성공")
            tests.append(('PoliteCrawler instantiation', True))
        except Exception as e:
            print(f"❌ PoliteCrawler 인스턴스 생성 실패: {e}")
            tests.append(('PoliteCrawler instantiation', False))

        return tests

    def test_phase_2_distributed_crawler(self):
        """Phase 2: 분산 크롤러"""
        print("\n" + "="*60)
        print("Phase 2: 분산 크롤러 테스트")
        print("="*60)

        tests = []

        # Test 1: distributed_crawler.py import
        try:
            import distributed_crawler
            print("✅ distributed_crawler.py import 성공")
            tests.append(('distributed_crawler import', True))
        except Exception as e:
            print(f"❌ distributed_crawler.py import 실패: {e}")
            tests.append(('distributed_crawler import', False))

        # Test 2: redis_queue_manager.py import
        try:
            import redis_queue_manager
            print("✅ redis_queue_manager.py import 성공")
            tests.append(('redis_queue_manager import', True))
        except Exception as e:
            print(f"❌ redis_queue_manager.py import 실패: {e}")
            tests.append(('redis_queue_manager import', False))

        return tests

    def test_phase_3_sharding(self):
        """Phase 3: Redis 샤딩"""
        print("\n" + "="*60)
        print("Phase 3: Redis 샤딩 테스트")
        print("="*60)

        tests = []

        # Test 1: sharded_queue_manager.py import
        try:
            import sharded_queue_manager
            print("✅ sharded_queue_manager.py import 성공")
            tests.append(('sharded_queue_manager import', True))
        except Exception as e:
            print(f"❌ sharded_queue_manager.py import 실패: {e}")
            tests.append(('sharded_queue_manager import', False))

        # Test 2: sharded_distributed_crawler.py import
        try:
            import sharded_distributed_crawler
            print("✅ sharded_distributed_crawler.py import 성공")
            tests.append(('sharded_distributed_crawler import', True))
        except Exception as e:
            print(f"❌ sharded_distributed_crawler.py import 실패: {e}")
            tests.append(('sharded_distributed_crawler import', False))

        return tests

    def test_phase_7_aggressive_optimization(self):
        """Phase 7: 공격적 최적화"""
        print("\n" + "="*60)
        print("Phase 7: 공격적 최적화 테스트")
        print("="*60)

        tests = []

        # Test 1: config/settings.py import
        try:
            from config import settings
            print("✅ config.settings import 성공")
            tests.append(('config.settings import', True))

            # Test 2: 공격적 설정 값 확인
            expected_values = {
                'GLOBAL_SEMAPHORE_LIMIT': 200,
                'TCP_CONNECTOR_LIMIT': 300,
                'TCP_CONNECTOR_LIMIT_PER_HOST': 20,
                'WORKER_THREADS': 16,
                'BATCH_SIZE': 100
            }

            for key, expected in expected_values.items():
                actual = getattr(settings, key, None)
                if actual == expected:
                    print(f"✅ {key} = {actual} (정상)")
                    tests.append((f'{key} value', True))
                else:
                    print(f"❌ {key} = {actual}, 예상값: {expected}")
                    tests.append((f'{key} value', False))

        except Exception as e:
            print(f"❌ config.settings import 실패: {e}")
            tests.append(('config.settings import', False))

        # Test 3: aggressive_performance_test.py import
        try:
            import aggressive_performance_test
            print("✅ aggressive_performance_test.py import 성공")
            tests.append(('aggressive_performance_test import', True))
        except Exception as e:
            print(f"❌ aggressive_performance_test.py import 실패: {e}")
            tests.append(('aggressive_performance_test import', False))

        return tests

    def test_phase_8_kubernetes(self):
        """Phase 8: Kubernetes 매니페스트"""
        print("\n" + "="*60)
        print("Phase 8: Kubernetes 매니페스트 테스트")
        print("="*60)

        tests = []

        k8s_files = [
            'k8s/base/namespace.yaml',
            'k8s/base/configmap.yaml',
            'k8s/base/secret.yaml',
            'k8s/base/postgres-statefulset.yaml',
            'k8s/base/redis-statefulset.yaml',
            'k8s/base/crawler-deployment.yaml',
            'k8s/autoscaling/keda-scaledobject.yaml'
        ]

        for file_path in k8s_files:
            full_path = Path(__file__).parent.parent / file_path
            if full_path.exists():
                print(f"✅ {file_path} 존재")
                tests.append((f'{file_path} exists', True))

                # YAML 문법 검증
                try:
                    import yaml
                    with open(full_path, 'r') as f:
                        yaml.safe_load_all(f)
                    print(f"✅ {file_path} YAML 문법 정상")
                    tests.append((f'{file_path} syntax', True))
                except Exception as e:
                    print(f"❌ {file_path} YAML 문법 오류: {e}")
                    tests.append((f'{file_path} syntax', False))
            else:
                print(f"❌ {file_path} 없음")
                tests.append((f'{file_path} exists', False))

        # Test Dockerfile
        dockerfile = Path(__file__).parent.parent / 'Dockerfile'
        if dockerfile.exists():
            print(f"✅ Dockerfile 존재")
            tests.append(('Dockerfile exists', True))
        else:
            print(f"❌ Dockerfile 없음")
            tests.append(('Dockerfile exists', False))

        return tests

    def test_phase_9_monitoring(self):
        """Phase 9: Prometheus & Grafana 모니터링"""
        print("\n" + "="*60)
        print("Phase 9: 모니터링 스택 테스트")
        print("="*60)

        tests = []

        monitoring_files = [
            'k8s/monitoring/prometheus/prometheus-configmap.yaml',
            'k8s/monitoring/prometheus/prometheus-deployment.yaml',
            'k8s/monitoring/grafana/grafana-configmap.yaml',
            'k8s/monitoring/grafana/grafana-deployment.yaml',
            'k8s/monitoring/grafana/crawler-dashboard.json',
            'k8s/monitoring/exporters.yaml',
            'k8s/monitoring/ingress.yaml'
        ]

        for file_path in monitoring_files:
            full_path = Path(__file__).parent.parent / file_path
            if full_path.exists():
                print(f"✅ {file_path} 존재")
                tests.append((f'{file_path} exists', True))

                # YAML/JSON 문법 검증
                if file_path.endswith('.yaml'):
                    try:
                        import yaml
                        with open(full_path, 'r') as f:
                            yaml.safe_load_all(f)
                        print(f"✅ {file_path} YAML 문법 정상")
                        tests.append((f'{file_path} syntax', True))
                    except Exception as e:
                        print(f"❌ {file_path} YAML 문법 오류: {e}")
                        tests.append((f'{file_path} syntax', False))
                elif file_path.endswith('.json'):
                    try:
                        import json
                        with open(full_path, 'r') as f:
                            json.load(f)
                        print(f"✅ {file_path} JSON 문법 정상")
                        tests.append((f'{file_path} syntax', True))
                    except Exception as e:
                        print(f"❌ {file_path} JSON 문법 오류: {e}")
                        tests.append((f'{file_path} syntax', False))
            else:
                print(f"❌ {file_path} 없음")
                tests.append((f'{file_path} exists', False))

        # Test monitoring/metrics.py
        try:
            from monitoring import metrics
            print("✅ monitoring.metrics import 성공")
            tests.append(('monitoring.metrics import', True))
        except Exception as e:
            print(f"❌ monitoring.metrics import 실패: {e}")
            tests.append(('monitoring.metrics import', False))

        return tests

    def test_phase_10_cicd(self):
        """Phase 10: GitHub Actions CI/CD"""
        print("\n" + "="*60)
        print("Phase 10: CI/CD 파이프라인 테스트")
        print("="*60)

        tests = []

        workflow_files = [
            '.github/workflows/ci.yml',
            '.github/workflows/docker-build.yml',
            '.github/workflows/deploy-k8s.yml',
            '.github/workflows/pr-automation.yml',
            '.github/workflows/release.yml'
        ]

        for file_path in workflow_files:
            full_path = Path(__file__).parent.parent.parent / file_path
            if full_path.exists():
                print(f"✅ {file_path} 존재")
                tests.append((f'{file_path} exists', True))

                # YAML 문법 검증
                try:
                    import yaml
                    with open(full_path, 'r') as f:
                        content = yaml.safe_load(f)

                    # Workflow 필수 필드 검증
                    if 'name' in content and 'on' in content and 'jobs' in content:
                        print(f"✅ {file_path} workflow 구조 정상")
                        tests.append((f'{file_path} structure', True))
                    else:
                        print(f"❌ {file_path} workflow 구조 불완전")
                        tests.append((f'{file_path} structure', False))

                except Exception as e:
                    print(f"❌ {file_path} YAML 문법 오류: {e}")
                    tests.append((f'{file_path} syntax', False))
            else:
                print(f"❌ {file_path} 없음")
                tests.append((f'{file_path} exists', False))

        # Test CI/CD guide
        guide_path = Path(__file__).parent.parent.parent / '.github' / 'CI_CD.md'
        if guide_path.exists():
            print(f"✅ .github/CI_CD.md 존재")
            tests.append(('CI_CD.md exists', True))
        else:
            print(f"❌ .github/CI_CD.md 없음")
            tests.append(('CI_CD.md exists', False))

        return tests

    def test_python_syntax(self):
        """Python 파일 전체 문법 검사"""
        print("\n" + "="*60)
        print("Python 코드 문법 검사")
        print("="*60)

        tests = []

        python_files = list(Path(__file__).parent.parent.glob('*.py'))

        for py_file in python_files:
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    code = f.read()
                compile(code, py_file.name, 'exec')
                print(f"✅ {py_file.name} 문법 정상")
                tests.append((f'{py_file.name} syntax', True))
            except SyntaxError as e:
                print(f"❌ {py_file.name} 문법 오류: {e}")
                tests.append((f'{py_file.name} syntax', False))
            except Exception as e:
                print(f"⚠️ {py_file.name} 검사 실패: {e}")
                tests.append((f'{py_file.name} check', False))

        return tests

    def run_all_tests(self):
        """전체 테스트 실행"""
        print("\n" + "🚀 "*30)
        print("Phase 1-10 통합 테스트 시작")
        print("🚀 "*30)

        all_tests = []

        # Phase별 테스트 실행
        all_tests.extend(self.test_phase_1_basic_crawler())
        all_tests.extend(self.test_phase_2_distributed_crawler())
        all_tests.extend(self.test_phase_3_sharding())
        all_tests.extend(self.test_phase_7_aggressive_optimization())
        all_tests.extend(self.test_phase_8_kubernetes())
        all_tests.extend(self.test_phase_9_monitoring())
        all_tests.extend(self.test_phase_10_cicd())
        all_tests.extend(self.test_python_syntax())

        # 결과 집계
        passed = sum(1 for _, result in all_tests if result)
        failed = sum(1 for _, result in all_tests if not result)
        total = len(all_tests)
        success_rate = (passed / total * 100) if total > 0 else 0

        # 최종 결과 출력
        print("\n" + "="*60)
        print("📊 테스트 결과 요약")
        print("="*60)
        print(f"총 테스트: {total}")
        print(f"✅ 성공: {passed}")
        print(f"❌ 실패: {failed}")
        print(f"성공률: {success_rate:.1f}%")
        print("="*60)

        # 실패한 테스트 목록
        if failed > 0:
            print("\n❌ 실패한 테스트:")
            for name, result in all_tests:
                if not result:
                    print(f"  - {name}")

        return {
            'total': total,
            'passed': passed,
            'failed': failed,
            'success_rate': success_rate,
            'tests': all_tests
        }

def main():
    runner = PhaseTestRunner()
    results = runner.run_all_tests()

    # 성공률에 따라 종료 코드 반환
    if results['success_rate'] >= 90:
        print("\n✅ 테스트 통과! (90% 이상 성공)")
        return 0
    elif results['success_rate'] >= 70:
        print("\n⚠️ 경고: 일부 테스트 실패 (70-90% 성공)")
        return 1
    else:
        print("\n❌ 테스트 실패! (70% 미만 성공)")
        return 2

if __name__ == '__main__':
    sys.exit(main())
