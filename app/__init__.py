from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# FastAPI 앱 생성
app = FastAPI(
    title="Investment Assistant API",
    description="실시간 투자 데이터 API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 루트 엔드포인트
@app.get("/")
async def root():
    return {
        "message": "Investment Assistant API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }

# Health Check 엔드포인트
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "investment-api"}

# 테스트 엔드포인트
@app.get("/api/test")
async def test_endpoint():
    return {"message": "FastAPI is working!", "timestamp": "2025-07-25"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)