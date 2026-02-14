"""Utilities for validating pgvector column dimension against embedder dimension."""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


async def get_page_chunks_embedding_dimension(conn) -> Optional[int]:
    """
    Return configured dimension of page_chunks.embedding vector column.

    pgvector stores vector(N) typmod directly as N (no offset).
    """
    row = await conn.fetchrow(
        """
        SELECT a.atttypmod
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = current_schema()
          AND c.relname = 'page_chunks'
          AND a.attname = 'embedding'
          AND a.attnum > 0
          AND NOT a.attisdropped
        """
    )
    if not row:
        return None

    atttypmod = row["atttypmod"]
    if atttypmod is None or atttypmod < 0:
        return None

    dim = int(atttypmod)
    return dim if dim > 0 else None


def ensure_dimension_match(
    *,
    component: str,
    model_name: str,
    model_dimension: int,
    db_dimension: Optional[int],
) -> None:
    """Raise ValueError when embedding model and DB vector dimensions mismatch."""
    if db_dimension is None:
        logger.warning(
            "%s: could not determine page_chunks.embedding dimension; skipping strict validation.",
            component,
        )
        return

    if db_dimension != model_dimension:
        raise ValueError(
            f"{component}: embedding dimension mismatch "
            f"(model={model_name}, dim={model_dimension}, db_vector_dim={db_dimension}). "
            "Use a compatible local model (e.g. all-MiniLM-L6-v2 for 384), "
            "or migrate/recreate page_chunks.embedding to the new vector dimension."
        )

