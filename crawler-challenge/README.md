# Crawler Challenge

목표: Python으로 28 pages/sec 달성, GIL 병목점 분석

## 설정 완료

## 1차 성능 테스트 결과

**테스트 환경:**
- 10개 URL 동시 크롤링
- jsonplaceholder.typicode.com API 엔드포인트
- 최대 동시 요청: 50개
- 요청 타임아웃: 10초

**결과:**
- **달성 성능: 12.53 pages/sec**
- 목표 대비: 44.8% (28 pages/sec 목표)
- 성공률: 100% (10/10 성공)
- 크롤링 시간: 0.80초

**메트릭 모니터링:**
- PPS: 실시간 처리율 추적 ✓
- CPU: psutil 활용 모니터링 ✓  
- Active Tasks: 동시 실행 태스크 수 ✓

**발견된 이슈:**
- Unicode 인코딩 문제 (Windows cp949) → 수정 완료
- httpbin.org 503 에러 → 안정적인 API로 변경
- Event loop 종료 경고 (무해함)

**다음 최적화 단계:**
1. 더 많은 URL로 스케일 테스트
2. 세션 재사용 최적화  
3. DNS 캐싱
4. HTTP/2 활용
5. GIL 병목점 분석