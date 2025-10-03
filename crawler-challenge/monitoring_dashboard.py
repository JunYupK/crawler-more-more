#!/usr/bin/env python3
"""
Monitoring Dashboard - 분산 크롤러 모니터링 대시보드
"""

import os
import time
import json
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify
from flask_socketio import SocketIO, emit
import redis
import psycopg2
import threading

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Redis 연결
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_client = redis.Redis(host=redis_host, port=6379, db=1, decode_responses=True)

# PostgreSQL 연결 설정
postgres_config = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': 5432,
    'database': os.getenv('POSTGRES_DB', 'crawler_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# HTML 템플릿
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>분산 크롤러 모니터링 대시보드</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; margin-bottom: 30px; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .stat-value { font-size: 2em; font-weight: bold; color: #2196F3; }
        .stat-label { color: #666; margin-top: 5px; }
        .chart-container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .worker-status { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }
        .worker-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .status-active { border-left: 4px solid #4CAF50; }
        .status-idle { border-left: 4px solid #FFC107; }
        .status-error { border-left: 4px solid #F44336; }
        .update-time { text-align: center; color: #666; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🕷️ 분산 크롤러 모니터링 대시보드</h1>
            <p>실시간 크롤링 성능 및 워커 상태 모니터링</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value" id="total-processed">0</div>
                <div class="stat-label">총 처리된 페이지</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="success-rate">0%</div>
                <div class="stat-label">성공률</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="pages-per-sec">0</div>
                <div class="stat-label">Pages/Sec</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="active-workers">0</div>
                <div class="stat-label">활성 워커</div>
            </div>
        </div>

        <div class="chart-container">
            <h3>실시간 처리량</h3>
            <canvas id="throughputChart" width="400" height="200"></canvas>
        </div>

        <div class="chart-container">
            <h3>큐 상태</h3>
            <canvas id="queueChart" width="400" height="200"></canvas>
        </div>

        <div class="chart-container">
            <h3>워커 상태</h3>
            <div class="worker-status" id="worker-status">
                <!-- 워커 상태는 JavaScript로 동적 생성 -->
            </div>
        </div>

        <div class="update-time">
            마지막 업데이트: <span id="last-update">-</span>
        </div>
    </div>

    <script>
        const socket = io();

        // 차트 초기화
        const throughputCtx = document.getElementById('throughputChart').getContext('2d');
        const throughputChart = new Chart(throughputCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Pages/Sec',
                    data: [],
                    borderColor: '#2196F3',
                    backgroundColor: 'rgba(33, 150, 243, 0.1)',
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });

        const queueCtx = document.getElementById('queueChart').getContext('2d');
        const queueChart = new Chart(queueCtx, {
            type: 'doughnut',
            data: {
                labels: ['완료', '대기', '처리중', '실패'],
                datasets: [{
                    data: [0, 0, 0, 0],
                    backgroundColor: ['#4CAF50', '#2196F3', '#FFC107', '#F44336']
                }]
            },
            options: { responsive: true }
        });

        // 소켓 이벤트 리스너
        socket.on('stats_update', function(data) {
            // 기본 통계 업데이트
            document.getElementById('total-processed').textContent = data.total_processed || 0;
            document.getElementById('success-rate').textContent = ((data.success_rate || 0) * 100).toFixed(1) + '%';
            document.getElementById('pages-per-sec').textContent = (data.pages_per_sec || 0).toFixed(2);
            document.getElementById('active-workers').textContent = data.active_workers || 0;

            // 처리량 차트 업데이트
            const now = new Date().toLocaleTimeString();
            throughputChart.data.labels.push(now);
            throughputChart.data.datasets[0].data.push(data.pages_per_sec || 0);

            if (throughputChart.data.labels.length > 20) {
                throughputChart.data.labels.shift();
                throughputChart.data.datasets[0].data.shift();
            }
            throughputChart.update();

            // 큐 차트 업데이트
            queueChart.data.datasets[0].data = [
                data.completed || 0,
                data.pending || 0,
                data.processing || 0,
                data.failed || 0
            ];
            queueChart.update();

            // 워커 상태 업데이트
            updateWorkerStatus(data.workers || []);

            // 업데이트 시간
            document.getElementById('last-update').textContent = new Date().toLocaleString();
        });

        function updateWorkerStatus(workers) {
            const container = document.getElementById('worker-status');
            container.innerHTML = '';

            workers.forEach(worker => {
                const div = document.createElement('div');
                div.className = `worker-card status-${worker.status}`;
                div.innerHTML = `
                    <h4>워커 ${worker.id}</h4>
                    <p>상태: ${worker.status}</p>
                    <p>처리량: ${worker.processed || 0}</p>
                    <p>성공률: ${(worker.success_rate * 100).toFixed(1)}%</p>
                `;
                container.appendChild(div);
            });
        }

        // 초기 데이터 요청
        socket.emit('request_update');
    </script>
</body>
</html>
"""

def get_queue_stats():
    """Redis 큐 상태 조회"""
    try:
        queues = {
            'priority_high': 'crawler:queue:priority_high',
            'priority_medium': 'crawler:queue:priority_medium',
            'priority_normal': 'crawler:queue:priority_normal',
            'priority_low': 'crawler:queue:priority_low'
        }

        stats = {}
        for queue_name, queue_key in queues.items():
            stats[f"queue_{queue_name}"] = redis_client.zcard(queue_key)

        stats['processing'] = redis_client.scard('crawler:processing')
        stats['completed'] = redis_client.scard('crawler:completed')
        stats['failed'] = redis_client.zcard('crawler:failed')

        stats['total_pending'] = sum(stats[k] for k in stats if k.startswith('queue_'))
        stats['total_urls'] = (stats['total_pending'] + stats['processing'] +
                             stats['completed'] + stats['failed'])

        if stats['total_urls'] > 0:
            stats['completion_rate'] = stats['completed'] / stats['total_urls']
        else:
            stats['completion_rate'] = 0.0

        return stats
    except Exception as e:
        print(f"큐 상태 조회 오류: {e}")
        return {}

def get_db_stats():
    """PostgreSQL 상태 조회"""
    try:
        conn = psycopg2.connect(**postgres_config)
        cursor = conn.cursor()

        # 최근 1시간 처리량
        cursor.execute("""
            SELECT COUNT(*) FROM crawled_pages
            WHERE crawled_at > NOW() - INTERVAL '1 hour'
        """)
        recent_count = cursor.fetchone()[0]

        # 총 페이지 수
        cursor.execute("SELECT COUNT(*) FROM crawled_pages")
        total_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            'total_pages': total_count,
            'recent_pages': recent_count
        }
    except Exception as e:
        print(f"DB 상태 조회 오류: {e}")
        return {'total_pages': 0, 'recent_pages': 0}

@app.route('/')
def dashboard():
    return render_template_string(DASHBOARD_TEMPLATE)

@socketio.on('request_update')
def handle_update_request():
    """클라이언트 업데이트 요청 처리"""
    send_stats_update()

def send_stats_update():
    """통계 업데이트 전송"""
    try:
        queue_stats = get_queue_stats()
        db_stats = get_db_stats()

        # 간단한 처리량 계산 (실제로는 더 정교한 계산 필요)
        pages_per_sec = queue_stats.get('completed', 0) / 3600 if queue_stats.get('completed', 0) > 0 else 0

        data = {
            'total_processed': queue_stats.get('completed', 0),
            'success_rate': queue_stats.get('completion_rate', 0),
            'pages_per_sec': pages_per_sec,
            'active_workers': 4,  # 하드코딩 (실제로는 워커 하트비트로 계산)
            'completed': queue_stats.get('completed', 0),
            'pending': queue_stats.get('total_pending', 0),
            'processing': queue_stats.get('processing', 0),
            'failed': queue_stats.get('failed', 0),
            'workers': [
                {'id': 1, 'status': 'active', 'processed': 25, 'success_rate': 0.8},
                {'id': 2, 'status': 'active', 'processed': 30, 'success_rate': 0.9},
                {'id': 3, 'status': 'active', 'processed': 22, 'success_rate': 0.75},
                {'id': 4, 'status': 'active', 'processed': 28, 'success_rate': 0.85},
            ]
        }

        socketio.emit('stats_update', data)
    except Exception as e:
        print(f"통계 업데이트 오류: {e}")

def background_updater():
    """백그라운드 업데이트 스레드"""
    while True:
        send_stats_update()
        time.sleep(5)  # 5초마다 업데이트

if __name__ == '__main__':
    print("모니터링 대시보드 시작...")
    print("대시보드 URL: http://localhost:8080")

    # 백그라운드 업데이트 스레드 시작
    update_thread = threading.Thread(target=background_updater, daemon=True)
    update_thread.start()

    socketio.run(app, host='0.0.0.0', port=8080, debug=False)