# ===== auto-restart-pod.sh =====
#!/bin/bash

# 백엔드 자동 재시작 스크립트
echo "🔧 Investment Backend 자동 재시작 시작..."

# 환경변수 설정
NAMESPACE="investment-assistant"
BACKEND_DEPLOYMENT="investment-api"

# 현재 시간
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

echo "📅 시작 시간: $TIMESTAMP"
echo "🏗️  네임스페이스: $NAMESPACE"
echo "🚀 배포명: $BACKEND_DEPLOYMENT"

# Pod 재시작
echo "🔄 백엔드 Pod 재시작 중..."
kubectl rollout restart deployment/$BACKEND_DEPLOYMENT -n $NAMESPACE

if [ $? -eq 0 ]; then
    echo "✅ 백엔드 재시작 명령 성공"
    
    # 재시작 상태 확인
    echo "⏳ 재시작 상태 확인 중..."
    kubectl rollout status deployment/$BACKEND_DEPLOYMENT -n $NAMESPACE --timeout=300s
    
    if [ $? -eq 0 ]; then
        echo "🎉 백엔드 재시작 완료!"
        echo "🌐 API 접속: https://api.investment-assistant.site/api/v1/"
        echo "🔍 로그 확인: kubectl logs -n $NAMESPACE deployment/$BACKEND_DEPLOYMENT -c fastapi --tail=20"
    else
        echo "❌ 백엔드 재시작 시간 초과"
        exit 1
    fi
else
    echo "❌ 백엔드 재시작 실패"
    exit 1
fi

echo "✅ 백엔드 자동 재시작 완료! ($(date +"%Y-%m-%d %H:%M:%S"))"
