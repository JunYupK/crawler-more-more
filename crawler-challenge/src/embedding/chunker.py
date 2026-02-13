"""
Markdown Chunker - 마크다운 텍스트 청킹
========================================

Markdown 문서를 RAG에 적합한 의미 단위 청크로 분할합니다.

분할 전략:
  1. 헤딩(#, ##, ###) 단위로 섹션 분리 → 의미 경계 보장
  2. 섹션이 MAX_CHARS 초과 시 문단(\n\n) 단위로 재분할
  3. 문단도 초과 시 문장 단위로 재분할
  4. MIN_CHARS 미만 청크 → 다음 청크와 병합

각 청크에 heading_ctx 포함 → RAG 결과 품질 향상
  예: "크롤러 아키텍처 > Kafka 토픽 설계"
"""

import re
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """단일 텍스트 청크"""
    text: str           # 임베딩 대상 텍스트 (heading_ctx 포함)
    heading_ctx: str    # 헤딩 경로 ("H1 제목 > H2 소제목")
    chunk_index: int    # 문서 내 순서 (0-based)


class MarkdownChunker:
    """
    Markdown → Chunk 리스트 분할기

    환경변수 대신 생성자 파라미터로 설정:
      max_chars: 청크 최대 문자 수 (기본 500)
      min_chars: 청크 최소 문자 수 (기본 50)
    """

    # 헤딩 파싱 패턴 (# ~ ######)
    HEADING_RE = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)

    def __init__(self, max_chars: int = 500, min_chars: int = 50):
        self.max_chars = max_chars
        self.min_chars = min_chars

    def split(self, markdown: str) -> list[Chunk]:
        """
        마크다운 텍스트를 청크 리스트로 분할

        Args:
            markdown: 처리할 마크다운 텍스트

        Returns:
            Chunk 리스트 (순서 보장, 빈 문서이면 빈 리스트)
        """
        if not markdown or not markdown.strip():
            return []

        # 헤딩 기반으로 섹션 분리
        sections = self._split_by_headings(markdown)

        # 각 섹션을 max_chars 기준으로 재분할
        raw_chunks: list[tuple[str, str]] = []  # (text, heading_ctx)
        for section_text, heading_ctx in sections:
            sub_chunks = self._split_section(section_text, heading_ctx)
            raw_chunks.extend(sub_chunks)

        # 너무 짧은 청크 병합
        merged = self._merge_short_chunks(raw_chunks)

        # Chunk 객체 생성
        return [
            Chunk(
                text=text.strip(),
                heading_ctx=ctx,
                chunk_index=i,
            )
            for i, (text, ctx) in enumerate(merged)
            if text.strip()
        ]

    # ------------------------------------------------------------------
    # 내부 메서드
    # ------------------------------------------------------------------

    def _split_by_headings(self, markdown: str) -> list[tuple[str, str]]:
        """
        헤딩 단위로 마크다운을 섹션으로 분리

        Returns:
            [(section_text, heading_ctx), ...]
        """
        sections: list[tuple[str, str]] = []
        heading_stack: list[tuple[int, str]] = []  # [(level, title), ...]

        # 헤딩 위치 파악
        boundaries: list[tuple[int, int, str]] = []  # (start, level, title)
        for m in self.HEADING_RE.finditer(markdown):
            level = len(m.group(1))
            title = m.group(2).strip()
            boundaries.append((m.start(), level, title))

        if not boundaries:
            # 헤딩 없는 문서 → 전체를 단일 섹션으로
            return [(markdown.strip(), '')]

        # 헤딩 이전 도입부 처리
        intro = markdown[:boundaries[0][0]].strip()
        if intro:
            sections.append((intro, ''))

        # 각 섹션 추출
        for i, (start, level, title) in enumerate(boundaries):
            # 다음 헤딩 시작 전까지가 이 섹션의 내용
            end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(markdown)
            section_raw = markdown[start:end]

            # 헤딩 스택 갱신 (현재 레벨보다 높거나 같은 헤딩 제거)
            heading_stack = [(l, t) for l, t in heading_stack if l < level]
            heading_stack.append((level, title))

            # heading_ctx 생성 ("H1 > H2 > ..." 형식)
            heading_ctx = ' > '.join(t for _, t in heading_stack)

            # 헤딩 라인 자체를 제거하고 본문만 추출
            body = self.HEADING_RE.sub('', section_raw, count=1).strip()
            if body:
                sections.append((body, heading_ctx))

        return sections

    def _split_section(self, text: str, heading_ctx: str) -> list[tuple[str, str]]:
        """
        섹션 텍스트를 max_chars 기준으로 분할

        분할 우선순위: 문단(\n\n) → 문장(. ) → 강제 분할
        """
        if len(text) <= self.max_chars:
            return [(text, heading_ctx)]

        chunks: list[tuple[str, str]] = []

        # 문단 단위 분할
        paragraphs = [p.strip() for p in re.split(r'\n\n+', text) if p.strip()]
        current = ''

        for para in paragraphs:
            if len(para) > self.max_chars:
                # 문단 자체가 너무 길면 문장 단위 분할
                if current:
                    chunks.append((current.strip(), heading_ctx))
                    current = ''
                sentence_chunks = self._split_by_sentences(para, heading_ctx)
                chunks.extend(sentence_chunks)
            elif len(current) + len(para) + 2 <= self.max_chars:
                current = (current + '\n\n' + para).strip() if current else para
            else:
                if current:
                    chunks.append((current.strip(), heading_ctx))
                current = para

        if current:
            chunks.append((current.strip(), heading_ctx))

        return chunks if chunks else [(text[:self.max_chars], heading_ctx)]

    def _split_by_sentences(self, text: str, heading_ctx: str) -> list[tuple[str, str]]:
        """
        문장 단위로 텍스트 분할 (마지막 수단)
        """
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks: list[tuple[str, str]] = []
        current = ''

        for sent in sentences:
            if len(current) + len(sent) + 1 <= self.max_chars:
                current = (current + ' ' + sent).strip() if current else sent
            else:
                if current:
                    chunks.append((current.strip(), heading_ctx))
                # 단일 문장이 max_chars 초과 시 강제 분할
                if len(sent) > self.max_chars:
                    for i in range(0, len(sent), self.max_chars):
                        chunks.append((sent[i:i + self.max_chars], heading_ctx))
                    current = ''
                else:
                    current = sent

        if current:
            chunks.append((current.strip(), heading_ctx))

        return chunks

    def _merge_short_chunks(
        self,
        chunks: list[tuple[str, str]],
    ) -> list[tuple[str, str]]:
        """
        min_chars 미만 청크를 인접 청크와 병합
        """
        if not chunks:
            return []

        merged: list[tuple[str, str]] = []
        pending_text, pending_ctx = chunks[0]

        for text, ctx in chunks[1:]:
            if len(pending_text) < self.min_chars:
                # 짧은 청크 → 다음 청크와 병합 (컨텍스트는 이전 것 유지)
                pending_text = (pending_text + '\n\n' + text).strip()
            else:
                merged.append((pending_text, pending_ctx))
                pending_text, pending_ctx = text, ctx

        # 마지막 청크 처리
        if pending_text:
            if merged and len(pending_text) < self.min_chars:
                last_text, last_ctx = merged[-1]
                merged[-1] = ((last_text + '\n\n' + pending_text).strip(), last_ctx)
            else:
                merged.append((pending_text, pending_ctx))

        return merged
