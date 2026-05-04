"""
[문제 요약]
1. 기존 main.py / main_accuracy_v4.py의 hana_on_predict()는 hybrid_retriever.retrievers[0] = BM25,
   hybrid_retriever.retrievers[1] = FAISS retriever 라는 '평면(flat) 구조'를 전제로 작성되어 있음.
2. ben_code.py는 KMS와 비KMS를 나누기 위해
   - KMS: ParentDocumentRetriever + BM25 앙상블
   - 비KMS: flat chunk 기반 BM25 + FAISS 앙상블
   - 최종: KMS 앙상블 + 비KMS 앙상블을 다시 앙상블
   하는 '중첩(nested) 구조'를 만듦.
3. 따라서 ben_code.py를 일부만 반영하면, hana_on_predict()는 여전히
   retrievers[1].vectorstore 같은 예전 구조를 기대하고,
   실제 retriever는 EnsembleRetriever를 반환하므로 Gradio 요청 시 500 에러가 날 수 있음.
4. 주피터에서는 함수 정의/초기화까지만 지나가면 에러가 안 보일 수 있지만,
   Gradio 서버는 실제 요청이 들어와 hana_on_predict()를 실행하는 순간 구조 불일치가 터짐.

[이번 파일의 목적]
- KMS와 나머지 3개(FAQ / official / 내규)를 분리해서 다루려는 ben_code.py의 목적은 유지한다.
- 대신 hana_on_predict()가 retriever 내부 구조에 직접 의존하지 않도록 바꾼다.
- 즉 "검색엔진 내부 구조"와 "응답 생성 로직" 사이의 결합을 느슨하게 만들어
  KMS 분리 구조에서도 안정적으로 동작하도록 맞춘다.

[적용 방식]
- 아래 코드 블록들을 main.py 또는 notebook 셀에 복붙해서 기존 정의를 덮어쓰는 방식으로 사용한다.
- 최소 필요 변경점:
  1) SplitHybridRetriever 클래스 추가
  2) ben 구조용 prepare_hybrid_retriever() 교체
  3) hana_on_predict()에서 debug candidate 수집 방식 교체
"""

import os
import re
import json
import pickle
from pathlib import Path
from typing import Any, Dict, List, Tuple

import faiss as faiss_lib
from openai import OpenAI
from langchain_core.embeddings import Embeddings
from langchain.storage import InMemoryStore
from langchain.text_splitter import RecursiveCharacterTextSplitter, MarkdownHeaderTextSplitter
from langchain_community.docstore.in_memory import InMemoryDocstore
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_community.retrievers import BM25Retriever
from langchain_community.vectorstores import FAISS
from langchain.retrievers import ParentDocumentRetriever, EnsembleRetriever


# =========================================================
# 기본 설정
# =========================================================

EMBED_BASE_URL = "http://10.84.116.20:43021/llm/bge-m3/v1"
EMBED_MODEL = "/models"

ACC_BM25_K = 12
ACC_VECTOR_K = 12
ACC_HYBRID_K = 12
ACC_FINAL_CONTEXT_K = 6

INTERNAL_RULE_DIR_NAME = "내규소수2_md"


class RemoteBgeEmbeddings(Embeddings):
    def __init__(self, base_url=EMBED_BASE_URL, model_name=EMBED_MODEL):
        self.client = OpenAI(base_url=base_url, api_key="dummy")
        self.model = model_name

    def embed_documents(self, texts):
        response = self.client.embeddings.create(model=self.model, input=texts)
        return [data.embedding for data in response.data]

    def embed_query(self, text):
        response = self.client.embeddings.create(model=self.model, input=[text])
        return response.data[0].embedding


# =========================================================
# 문서 메타데이터 / 공통 유틸
# =========================================================

def parse_simple_front_matter(text):
    if not text.startswith("---"):
        return {}, text

    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text

    end_idx = None
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            end_idx = i
            break

    if end_idx is None:
        return {}, text

    metadata = {}
    for line in lines[1:end_idx]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        metadata[key] = value

    body = "\n".join(lines[end_idx + 1:]).lstrip()
    return metadata, body


def is_internal_rule_doc(doc):
    source = str(doc.metadata.get("source", "")).replace("\\", "/")
    file_name = os.path.basename(source)
    return (
        INTERNAL_RULE_DIR_NAME in source
        or bool(re.match(r"^\d+\.\d+(?:\.\d+)?\.md$", file_name, re.IGNORECASE))
        or doc.metadata.get("doc_type") == "internal_rule"
    )


def enrich_internal_rule_doc(doc):
    front_matter, body = parse_simple_front_matter(doc.page_content)
    if front_matter:
        doc.metadata.update(front_matter)
        doc.page_content = body

    if is_internal_rule_doc(doc):
        doc.metadata["doc_type"] = doc.metadata.get("doc_type", "internal_rule")

    return doc


def build_doc_key(doc, fallback_rank=1):
    source = doc.metadata.get("source", "unknown_source")
    chunk_id = doc.metadata.get("chunk_id", fallback_rank)
    return f"{source}::chunk::{chunk_id}"


def classify_source_type(doc):
    if doc.metadata.get("doc_type") == "internal_rule" or is_internal_rule_doc(doc):
        return "internal_rule"

    source = str(doc.metadata.get("source", "")).replace("\\", "/").lower()
    if "official_md" in source:
        return "official"
    if "faq_output_md" in source:
        return "faq"
    if "kms_output_md" in source:
        return "kms"
    return "other"


def format_doc_display(doc, fallback_rank=1):
    if doc.metadata.get("doc_type") == "internal_rule":
        doc_code = str(doc.metadata.get("doc_code", "")).strip()
        section_no = str(doc.metadata.get("section_no", "")).strip()
        section_title = str(doc.metadata.get("section_title", "")).strip()
        parent_section_title = str(doc.metadata.get("parent_section_title", "")).strip()

        if doc_code and section_no and parent_section_title and section_title and parent_section_title != section_title:
            return f"{doc_code} {section_no} {parent_section_title} > {section_title}"
        if doc_code and section_no and section_title:
            return f"{doc_code} {section_no} {section_title}"
        if section_no and section_title:
            return f"{section_no} {section_title}"
        if section_no:
            return section_no

    file_name = os.path.basename(doc.metadata.get("source", "KMS"))
    chunk_num = doc.metadata.get("chunk_id", fallback_rank)
    return f"{file_name}#{chunk_num}"


# =========================================================
# ben 구조 핵심: KMS / non-KMS 분리한 retriever wrapper
# =========================================================

def _parse_frontmatter(text: str) -> Tuple[dict, str]:
    fm_match = re.match(r"^---\n(.*?)\n---\n?", text, re.DOTALL)
    if not fm_match:
        return {}, text

    fm_text = fm_match.group(1)
    body = text[fm_match.end():]

    try:
        import yaml
        metadata = yaml.safe_load(fm_text) or {}
        return metadata, body
    except Exception:
        pass

    metadata = {}
    current_list_key = None
    for line in fm_text.splitlines():
        list_match = re.match(r'^\s+- "(.*)"$', line)
        if list_match and current_list_key:
            metadata[current_list_key].append(list_match.group(1))
            continue

        list_key_match = re.match(r"^(\S+):$", line)
        if list_key_match:
            current_list_key = list_key_match.group(1)
            metadata[current_list_key] = []
            continue

        kv_match = re.match(r'^([^:]+):\s*"?(.*?)"?$', line)
        if kv_match:
            current_list_key = None
            key = kv_match.group(1).strip()
            val = kv_match.group(2).strip().strip('"')
            metadata[key] = val

    return metadata, body


def _load_parent_docs(md_dir: str) -> list:
    headers_to_split_on = [("#", "title"), ("##", "section"), ("###", "subsection")]
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False,
    )

    md_path = Path(md_dir)
    parent_docs = []
    for md_file in md_path.glob("*.md"):
        try:
            text = md_file.read_text(encoding="utf-8")
            meta, body = _parse_frontmatter(text)
            meta["file_name"] = md_file.name
            category_str = meta.get("분류", "")
            if category_str and category_str != "분류 없음":
                for depth, part in enumerate(category_str.split(" > "), start=1):
                    meta[f"category_depth{depth}"] = part.strip()

            sections = header_splitter.split_text(body)
            for section in sections:
                section.metadata.update(meta)
                section.metadata["source_group"] = "kms"
            parent_docs.extend(sections)
        except Exception as e:
            print(f"⚠️ {md_file.name} 처리 실패: {e}")
    return parent_docs

'''
1. 월세
2. 관리비
3. 전기세/가스비/수도요금
4. 통신비(핸드폰 + 인터넷)
5. 이발비

? E아브레유 E하퍼

E슈미트 E롤린스 E어틀리 R하워드

R리얼무토

E약사님 R정수빈 B박건우

B김동주 R김재호 R오재원 R오재일

E양의지 / L민병헌

B캐넌 E라뱅 E홍창기

R문보경 B오지환 R신민재 R오스틴

E강민호 / L박용택

B최형우 E김성윤 E구자욱orE양준혁

E박석민 R이재현/구자욱 용병 R디아즈

E이만수 L이승엽

벨린저 미키맨틀 판사님

약사님 지터 소리아노 테세이라

포사다 / 로저메리스

쉐필드 캠프 벨린저

벨트레 약사님-바윗주 베츠 프리먼

피아자 / 오오타니

'''

def build_kms_retriever(
    md_dir: str = "kms_output_md",
    embeddings=None,
    faiss_save_path: str = "faiss_index",
    docstore_save_path: str = "docstore.pkl",
    parents_save_path: str = "parent_docs.pkl",
    child_chunk_size: int = 800,
    child_chunk_overlap: int = 100,
    dense_weight: float = 0.5,
    sparse_weight: float = 0.5,
    bm25_k: int = ACC_BM25_K,
):
    if embeddings is None:
        raise ValueError("embeddings is required")

    parent_docs = _load_parent_docs(md_dir)
    sample_dim = len(embeddings.embed_query("test"))
    index = faiss_lib.IndexFlatL2(sample_dim)
    vectorstore = FAISS(
        embedding_function=embeddings.embed_query,
        index=index,
        docstore=InMemoryDocstore(),
        index_to_docstore_id={},
    )

    child_splitter = RecursiveCharacterTextSplitter(
        chunk_size=child_chunk_size,
        chunk_overlap=child_chunk_overlap,
        separators=["\n\n", "\n", "。", ". ", " ", ""],
    )
    docstore = InMemoryStore()
    dense_retriever = ParentDocumentRetriever(
        vectorstore=vectorstore,
        docstore=docstore,
        child_splitter=child_splitter,
    )
    dense_retriever.add_documents(parent_docs)

    bm25_retriever = BM25Retriever.from_documents(parent_docs)
    bm25_retriever.k = bm25_k

    ensemble_retriever = EnsembleRetriever(
        retrievers=[dense_retriever, bm25_retriever],
        weights=[dense_weight, sparse_weight],
    )

    # Persist KMS side so internal notebook can reuse it if needed.
    vectorstore.save_local(faiss_save_path)
    with open(docstore_save_path, "wb") as f:
        pickle.dump(dict(docstore.store), f)
    with open(parents_save_path, "wb") as f:
        pickle.dump(parent_docs, f)

    return {
        "ensemble": ensemble_retriever,
        "dense_retriever": dense_retriever,
        "bm25_retriever": bm25_retriever,
        "parent_docs": parent_docs,
    }


def load_kms_retriever(
    embeddings=None,
    faiss_save_path: str = "faiss_index",
    docstore_save_path: str = "docstore.pkl",
    parents_save_path: str = "parent_docs.pkl",
    child_chunk_size: int = 800,
    child_chunk_overlap: int = 100,
    dense_weight: float = 0.5,
    sparse_weight: float = 0.5,
    bm25_k: int = ACC_BM25_K,
):
    if embeddings is None:
        raise ValueError("embeddings is required")

    vectorstore = FAISS.load_local(
        faiss_save_path,
        embeddings,
        allow_dangerous_deserialization=True,
    )
    with open(docstore_save_path, "rb") as f:
        store_data = pickle.load(f)

    docstore = InMemoryStore()
    docstore.mset(list(store_data.items()))

    child_splitter = RecursiveCharacterTextSplitter(
        chunk_size=child_chunk_size,
        chunk_overlap=child_chunk_overlap,
        separators=["\n\n", "\n", "。", ". ", " ", ""],
    )
    dense_retriever = ParentDocumentRetriever(
        vectorstore=vectorstore,
        docstore=docstore,
        child_splitter=child_splitter,
    )

    with open(parents_save_path, "rb") as f:
        parent_docs = pickle.load(f)

    bm25_retriever = BM25Retriever.from_documents(parent_docs)
    bm25_retriever.k = bm25_k

    ensemble_retriever = EnsembleRetriever(
        retrievers=[dense_retriever, bm25_retriever],
        weights=[dense_weight, sparse_weight],
    )

    return {
        "ensemble": ensemble_retriever,
        "dense_retriever": dense_retriever,
        "bm25_retriever": bm25_retriever,
        "parent_docs": parent_docs,
    }


class SplitHybridRetriever:
    """
    KMS / non-KMS 구조를 유지하면서도, hana_on_predict()는 내부 구현에 직접 의존하지 않도록
    공통 인터페이스를 제공하는 wrapper.
    """

    def __init__(self, kms_bundle: Dict[str, Any], non_kms_bundle: Dict[str, Any], top_ensemble: EnsembleRetriever):
        self.kms = kms_bundle
        self.non_kms = non_kms_bundle
        self.top_ensemble = top_ensemble

    def invoke(self, query: str):
        return self.top_ensemble.invoke(query)

    def get_debug_candidates(self, query: str, vector_k: int = ACC_VECTOR_K) -> Dict[str, Any]:
        # KMS는 nested 구조라 bm25/vector를 완벽히 flat처럼 뽑기보다,
        # "kms top candidates"와 "non-kms sparse/dense/hybrid"를 나눠 진단하는 방식으로 제공.
        kms_docs = self.kms["ensemble"].invoke(query)

        non_kms_bm25_docs = self.non_kms["bm25_retriever"].invoke(query)
        non_kms_vector_docs = self.non_kms["faiss_retriever"].vectorstore.similarity_search_with_score(query, k=vector_k)
        non_kms_hybrid_docs = self.non_kms["ensemble"].invoke(query)
        top_hybrid_docs = self.top_ensemble.invoke(query)

        return {
            "kms_docs": kms_docs,
            "non_kms_bm25_docs": non_kms_bm25_docs,
            "non_kms_vector_docs": non_kms_vector_docs,
            "non_kms_hybrid_docs": non_kms_hybrid_docs,
            "top_hybrid_docs": top_hybrid_docs,
        }


def prepare_hybrid_retriever(
    non_kms_paths=None,
    kms_md_dir="kms_output_md",
    kms_faiss_path="faiss_index",
    kms_docstore_path="docstore.pkl",
    kms_parents_path="parent_docs.pkl",
):
    """
    ben_code.py의 목적을 유지:
    - KMS는 ParentDocumentRetriever 기반으로 분리
    - FAQ/official/내규는 flat chunk 기반으로 별도 처리
    - 마지막에 둘을 합친다
    """
    if non_kms_paths is None:
        non_kms_paths = ["./faq_output_md", "./official_md", "./내규_md"]

    remote_embeddings = RemoteBgeEmbeddings(base_url=EMBED_BASE_URL, model_name=EMBED_MODEL)

    try:
        print("Loading KMS split retriever...")
        if (
            os.path.exists(kms_faiss_path)
            and os.path.exists(kms_docstore_path)
            and os.path.exists(kms_parents_path)
        ):
            kms_bundle = load_kms_retriever(
                embeddings=remote_embeddings,
                faiss_save_path=kms_faiss_path,
                docstore_save_path=kms_docstore_path,
                parents_save_path=kms_parents_path,
            )
        else:
            kms_bundle = build_kms_retriever(
                md_dir=kms_md_dir,
                embeddings=remote_embeddings,
                faiss_save_path=kms_faiss_path,
                docstore_save_path=kms_docstore_path,
                parents_save_path=kms_parents_path,
            )

        all_splits = []
        kms_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
        faq_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)
        rule_overflow_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=200)

        for path in non_kms_paths:
            if not os.path.exists(path):
                continue

            print(f"Loading non-KMS path: {path}")
            loader = DirectoryLoader(
                path,
                glob="*.md",
                loader_cls=TextLoader,
                loader_kwargs={"encoding": "utf-8", "autodetect_encoding": True},
            )
            documents = loader.load()
            documents = [enrich_internal_rule_doc(doc) for doc in documents]

            is_faq_path = "faq" in path.lower()
            is_rule_path = INTERNAL_RULE_DIR_NAME in path

            for doc in documents:
                doc.metadata["source_group"] = "non_kms"
                if is_rule_path and is_internal_rule_doc(doc):
                    if len(doc.page_content) <= 3000:
                        doc.metadata["chunk_id"] = 1
                        all_splits.append(doc)
                    else:
                        file_splits = rule_overflow_splitter.split_documents([doc])
                        for i, split in enumerate(file_splits):
                            split.metadata["chunk_id"] = i + 1
                            split.metadata["source_group"] = "non_kms"
                            all_splits.append(split)
                else:
                    current_splitter = faq_splitter if is_faq_path else kms_splitter
                    file_splits = current_splitter.split_documents([doc])
                    for i, split in enumerate(file_splits):
                        split.metadata["chunk_id"] = i + 1
                        split.metadata["source_group"] = "non_kms"
                        all_splits.append(split)

        print(f"Embedding non-KMS chunks: {len(all_splits)}")
        vectorstore = FAISS.from_documents(all_splits, remote_embeddings)
        non_kms_faiss = vectorstore.as_retriever(search_kwargs={"k": ACC_HYBRID_K})
        non_kms_bm25 = BM25Retriever.from_documents(all_splits)
        non_kms_bm25.k = ACC_BM25_K
        non_kms_ensemble = EnsembleRetriever(
            retrievers=[non_kms_bm25, non_kms_faiss],
            weights=[0.3, 0.7],
        )

        non_kms_bundle = {
            "bm25_retriever": non_kms_bm25,
            "faiss_retriever": non_kms_faiss,
            "ensemble": non_kms_ensemble,
        }

        top_ensemble = EnsembleRetriever(
            retrievers=[kms_bundle["ensemble"], non_kms_bundle["ensemble"]],
            weights=[0.5, 0.5],
        )

        print("SplitHybridRetriever ready (KMS / non-KMS)")
        return SplitHybridRetriever(kms_bundle, non_kms_bundle, top_ensemble)

    except Exception as e:
        print(f"Retriever build error: {str(e)}")
        return None


# =========================================================
# hana_on_predict 쪽에서 쓸 디버그 후보 수집 유틸
# =========================================================

def get_retrieval_debug_info(hybrid_retriever, search_query: str) -> Dict[str, Any]:
    """
    hana_on_predict()가 retriever 내부 구조를 직접 알 필요 없게 하는 어댑터 함수.

    - SplitHybridRetriever이면 KMS / non-KMS를 나눠 반환
    - 기존 flat EnsembleRetriever면 예전 방식으로 반환
    """
    if hasattr(hybrid_retriever, "get_debug_candidates"):
        return hybrid_retriever.get_debug_candidates(search_query, vector_k=ACC_VECTOR_K)

    # fallback: 기존 main.py / main_accuracy_v4.py 구조
    bm25_retriever = hybrid_retriever.retrievers[0]
    faiss_retriever = hybrid_retriever.retrievers[1]
    return {
        "bm25_docs": bm25_retriever.invoke(search_query),
        "vector_docs": faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=ACC_VECTOR_K),
        "top_hybrid_docs": hybrid_retriever.invoke(search_query),
    }


# =========================================================
# hana_on_predict 교체 시 수정해야 할 핵심 포인트 안내
# =========================================================
#
# 1. 기존의 아래 코드는 더 이상 직접 쓰지 말 것.
#
#    bm25_retriever = hybrid_retriever.retrievers[0]
#    faiss_retriever = hybrid_retriever.retrievers[1]
#    bm25_candidates = bm25_retriever.invoke(search_query)
#    vector_candidates = faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=...)
#
# 2. 대신 아래처럼 바꿔야 함.
#
#    debug_info = get_retrieval_debug_info(hybrid_retriever, search_query)
#
#    if "bm25_docs" in debug_info:
#        # 기존 flat 구조
#        bm25_candidates = debug_info["bm25_docs"]
#        vector_candidates = debug_info["vector_docs"]
#        hybrid_candidates = debug_info["top_hybrid_docs"]
#    else:
#        # split 구조
#        kms_candidates = debug_info["kms_docs"]
#        non_kms_bm25_candidates = debug_info["non_kms_bm25_docs"]
#        non_kms_vector_candidates = debug_info["non_kms_vector_docs"]
#        hybrid_candidates = debug_info["top_hybrid_docs"]
#
# 3. split 구조에서는 diagnostic log도 "bm25/vector 1벌" 전제가 아니라
#    "KMS 후보 / non-KMS sparse / non-KMS dense / 최종 hybrid" 구조로 바꾸는 것이 좋음.
#
# 4. 즉 ben_code.py의 목적을 유지하려면
#    prepare_hybrid_retriever만 바꾸면 안 되고,
#    hana_on_predict가 retriever 내부를 직접 파고들지 않게 바꾸는 것이 핵심이다.

