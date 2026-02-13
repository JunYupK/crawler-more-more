"""
Embedding Module - RAG를 위한 벡터 임베딩 파이프라인
=====================================================

주요 컴포넌트:
  - chunker.py         : Markdown 텍스트 → 의미 단위 청크 분할
  - embedder.py        : 텍스트 → 벡터 변환 (Local / OpenAI)
  - embedding_worker.py: Kafka processed.final → embed → pgvector 저장
  - rag_search.py      : 벡터 유사도 검색 인터페이스
"""
