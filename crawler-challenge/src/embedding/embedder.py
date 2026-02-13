"""
Embedder - 텍스트 → 벡터 변환 모델 추상화
==========================================

지원 백엔드:
  local  : sentence-transformers (기본값, 오프라인 가능)
  openai : OpenAI Embeddings API (OPENAI_API_KEY 필요)

환경변수:
  EMBED_BACKEND      : "local" | "openai" (기본: local)
  EMBED_MODEL_NAME   : 모델명
                       local  → "all-MiniLM-L6-v2" (384차원)
                       openai → "text-embedding-3-small" (1536차원)
  OPENAI_API_KEY     : OpenAI API 키 (EMBED_BACKEND=openai 시 필수)
  EMBED_BATCH_SIZE   : 임베딩 배치 크기 (기본: 32)

모델별 권장 용도:
  all-MiniLM-L6-v2   : 384차원, 빠름, 일반 용도 (기본)
  all-mpnet-base-v2  : 768차원, 품질↑, 속도↓
  text-embedding-3-small : 1536차원, 최고 품질, API 비용 발생
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


class BaseEmbedder(ABC):
    """임베딩 모델 추상 인터페이스"""

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        텍스트 목록을 벡터 목록으로 변환

        Args:
            texts: 임베딩할 텍스트 목록

        Returns:
            각 텍스트에 대응하는 float 벡터 목록
            shape: (len(texts), self.dimension)
        """

    @property
    @abstractmethod
    def dimension(self) -> int:
        """임베딩 벡터 차원 수"""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """사용 중인 모델명"""


class SentenceTransformerEmbedder(BaseEmbedder):
    """
    로컬 sentence-transformers 기반 임베딩

    모델 다운로드: 첫 실행 시 HuggingFace에서 자동 다운로드
    캐시 위치: ~/.cache/huggingface/hub/
    """

    DEFAULT_MODEL = "all-MiniLM-L6-v2"

    # 모델별 차원 수 (자동 감지 실패 시 폴백)
    KNOWN_DIMENSIONS = {
        "all-MiniLM-L6-v2": 384,
        "all-MiniLM-L12-v2": 384,
        "all-mpnet-base-v2": 768,
        "paraphrase-multilingual-MiniLM-L12-v2": 384,
        "paraphrase-multilingual-mpnet-base-v2": 768,
        "BAAI/bge-small-en-v1.5": 384,
        "BAAI/bge-base-en-v1.5": 768,
    }

    def __init__(self, model_name: Optional[str] = None, device: Optional[str] = None):
        """
        Args:
            model_name: sentence-transformers 모델명 (기본: all-MiniLM-L6-v2)
            device: 추론 디바이스 ("cpu", "cuda", "mps", None=자동)
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers 패키지가 필요합니다.\n"
                "설치: pip install sentence-transformers"
            )

        self._model_name = model_name or os.getenv("EMBED_MODEL_NAME", self.DEFAULT_MODEL)
        self._device = device

        logger.info(f"Loading sentence-transformers model: {self._model_name}")
        self._model = SentenceTransformer(self._model_name, device=self._device)

        # 차원 수 확인
        self._dimension = self._model.get_sentence_embedding_dimension()
        if self._dimension is None:
            self._dimension = self.KNOWN_DIMENSIONS.get(self._model_name, 384)
            logger.warning(
                f"Could not auto-detect dimension for {self._model_name}, "
                f"using {self._dimension}"
            )

        logger.info(
            f"SentenceTransformerEmbedder ready: "
            f"model={self._model_name}, dim={self._dimension}"
        )

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        배치 임베딩 (sentence-transformers 내부 배치 처리 활용)

        Args:
            texts: 임베딩할 텍스트 목록

        Returns:
            numpy array → Python float 리스트 변환
        """
        if not texts:
            return []

        embeddings = self._model.encode(
            texts,
            batch_size=int(os.getenv("EMBED_BATCH_SIZE", "32")),
            show_progress_bar=False,
            normalize_embeddings=True,  # cosine similarity 최적화
            convert_to_numpy=True,
        )
        return embeddings.tolist()

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name


class OpenAIEmbedder(BaseEmbedder):
    """
    OpenAI Embeddings API 기반 임베딩

    필수: OPENAI_API_KEY 환경변수 설정
    """

    DEFAULT_MODEL = "text-embedding-3-small"

    KNOWN_DIMENSIONS = {
        "text-embedding-3-small": 1536,
        "text-embedding-3-large": 3072,
        "text-embedding-ada-002": 1536,
    }

    def __init__(self, model_name: Optional[str] = None, api_key: Optional[str] = None):
        """
        Args:
            model_name: OpenAI 임베딩 모델명
            api_key: OpenAI API 키 (없으면 OPENAI_API_KEY 환경변수 사용)
        """
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError(
                "openai 패키지가 필요합니다.\n"
                "설치: pip install openai"
            )

        self._model_name = model_name or os.getenv("EMBED_MODEL_NAME", self.DEFAULT_MODEL)
        _api_key = api_key or os.getenv("OPENAI_API_KEY")

        if not _api_key:
            raise ValueError(
                "OPENAI_API_KEY 환경변수 또는 api_key 파라미터가 필요합니다."
            )

        from openai import OpenAI
        self._client = OpenAI(api_key=_api_key)
        self._dimension = self.KNOWN_DIMENSIONS.get(self._model_name, 1536)

        logger.info(
            f"OpenAIEmbedder ready: "
            f"model={self._model_name}, dim={self._dimension}"
        )

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        OpenAI API 배치 임베딩

        주의: API 요금 발생 / 네트워크 필요
        OpenAI API는 단일 호출로 최대 2048개 텍스트 처리 가능
        """
        if not texts:
            return []

        # 빈 텍스트 처리 (API 오류 방지)
        cleaned = [t if t.strip() else " " for t in texts]

        try:
            response = self._client.embeddings.create(
                model=self._model_name,
                input=cleaned,
                encoding_format="float",
            )
            return [item.embedding for item in response.data]

        except Exception as e:
            logger.error(f"OpenAI embedding API error: {e}")
            raise

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name


def create_embedder() -> BaseEmbedder:
    """
    환경변수 EMBED_BACKEND에 따라 적절한 Embedder 인스턴스 생성

    EMBED_BACKEND=local   → SentenceTransformerEmbedder (기본값)
    EMBED_BACKEND=openai  → OpenAIEmbedder

    Returns:
        BaseEmbedder 구현체
    """
    backend = os.getenv("EMBED_BACKEND", "local").lower()

    if backend == "openai":
        logger.info("Creating OpenAI embedder")
        return OpenAIEmbedder()
    else:
        if backend != "local":
            logger.warning(
                f"Unknown EMBED_BACKEND='{backend}', falling back to 'local'"
            )
        logger.info("Creating local SentenceTransformer embedder")
        return SentenceTransformerEmbedder()
