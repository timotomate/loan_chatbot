###################
'''
1. 검색DB(여기서부터 시작)
- 내규 전용 처리 유틸 신규 생성
'''
####################
import re

INTERNAL_RULE_DIR_NAME = "내규소수2_md"


def parse_simple_front_matter(text):
    """
    split_ld_rules_to_md*.py가 만든 YAML front matter를 간단 파싱한다.
    외부 yaml 패키지 없이 key: value 형태만 읽는다.
    """
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
    """
    문서 출처를 internal_rule / official / faq / kms 로 분류한다.
    """
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


import os

from langchain_core.embeddings import Embeddings
from openai import OpenAI
from langchain_community.document_loaders import DirectoryLoader, TextLoader ,UnstructuredMarkdownLoader #os 패키지 설치 필요
# from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_community.vectorstores import FAISS

class RemoteBgeEmbeddings(Embeddings):
    def __init__(self, base_url, model_name):
        self.client = OpenAI(base_url=base_url, api_key="dummy")
        self.model = model_name

    def embed_documents(self, texts):
        # 여러 문서를 한꺼번에 임베딩
        response = self.client.embeddings.create(
            model=self.model,
            input=texts
        )
        return [data.embedding for data in response.data]

    def embed_query(self, text):
        # 질문(단일 문장) 임베딩
        response = self.client.embeddings.create(
            model=self.model,
            input=[text]
        )
        return response.data[0].embedding


###################
'''
2. hynrid DB
- bm25 3: vector 7
- 참조문서 5개로 제한
'''
###################

# [NEW ACCURACY] ==========================================
# Accuracy-oriented overrides start here.
# main.py is left untouched; this file redefines only the pieces we want to improve.
# =========================================================
import json

# [NEW ACCURACY] Separate retrieval breadth from final context size.
ACC_BM25_K = 12
ACC_VECTOR_K = 12
ACC_HYBRID_K = 12
ACC_FINAL_CONTEXT_K = 6

PROPERTY_HINT_MAP = {
    "대출한도": ["한도", "최대한도", "최대 금액", "최대금액", "얼마", "억원", "금액"],
    "대출대상": ["대상", "대출대상", "대상자", "누가", "자격"],
    "대상주택": ["대상주택", "집 조건", "물건 조건", "주택 조건", "가능한 주택", "주택요건"],
    "대출금리": ["금리", "이율", "대출금리"],
    "심사 및 승인": ["승인", "승인권자", "누가 승인", "결재", "전결", "심사"],
    "채권보전": ["채권보전", "담보", "보전"],
    "중도상환해약금": ["중도상환", "중도상환해약금", "중도상환수수료"],
}

PRODUCT_HINT_TOKENS = [
    "우량주택전세론", "버팀목", "디딤돌", "전세자금대출", "전세자금", "전세론",
    "주신보", "보증부월세", "오피스텔", "디딤돌대출", "중소기업취업청년",
]


def format_doc_display(doc, fallback_rank=1):
    # [CHANGED ACCURACY] Include parent section context for internal rules when available.
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


def infer_question_profile(message):
    # [CHANGED ACCURACY] Explicit profile includes comparison intent and normalized properties.
    text = str(message or "").strip()
    lower_text = text.lower()

    rule_keywords = [
        "한도", "최대한도", "최대 금액", "최대금액", "얼마", "금리", "대상", "대출대상",
        "대상주택", "채권보전", "승인", "심사", "중도상환", "기한연장", "요건", "조건",
    ]
    system_keywords = [
        "전산", "화면", "입력", "등록", "처리", "메뉴", "#", "화면번호",
    ]
    case_keywords = [
        "가능", "가능한가", "경우", "케이스", "상황", "예외", "상담", "처리해야",
    ]
    official_keywords = [
        "hug", "hf", "sgi", "주택도시보증공사", "주택금융공사", "서울보증보험", "보증기관",
    ]
    compare_keywords = [
        "비교", "각각", "기관별", "보증기관별", "상품별",
    ]

    def score_keywords(keywords):
        return sum(1 for kw in keywords if kw.lower() in lower_text)

    institutions = []
    if "hug" in lower_text:
        institutions.append("HUG")
    if "hf" in lower_text:
        institutions.append("HF")
    if "sgi" in lower_text:
        institutions.append("SGI")

    normalized_properties = []
    for canonical_name, aliases in PROPERTY_HINT_MAP.items():
        if any(alias.lower() in lower_text for alias in aliases):
            normalized_properties.append(canonical_name)

    product_hits = [token for token in PRODUCT_HINT_TOKENS if token.lower() in lower_text]
    compare_score = score_keywords(compare_keywords)
    if len(institutions) >= 2:
        compare_score += 2

    return {
        "raw_text": text,
        "rule_score": score_keywords(rule_keywords) + len(product_hits),
        "system_score": score_keywords(system_keywords),
        "case_score": score_keywords(case_keywords),
        "official_score": score_keywords(official_keywords),
        "compare_score": compare_score,
        "is_compare": compare_score >= 1 or len(institutions) >= 2,
        "institutions": institutions,
        "normalized_properties": normalized_properties,
        "product_hits": product_hits,
    }


def detect_doc_institutions(doc):
    # [NEW ACCURACY] Used for compare-question coverage.
    haystack = " ".join(
        [
            str(doc.metadata.get("source", "")),
            str(doc.metadata.get("section_title", "")),
            str(doc.metadata.get("parent_section_title", "")),
            str(doc.metadata.get("doc_title", "")),
            str(doc.page_content or "")[:1200],
        ]
    ).lower()
    found = []
    if "hug" in haystack or "주택도시보증공사" in haystack:
        found.append("HUG")
    if "hf" in haystack or "주택금융공사" in haystack or "주신보" in haystack:
        found.append("HF")
    if "sgi" in haystack or "서울보증보험" in haystack:
        found.append("SGI")
    return found


def _score_rerank_item(doc, idx, message, profile):
    # [NEW ACCURACY] Reason-aware rerank scoring.
    source_type = classify_source_type(doc)
    score = 0.0
    reasons = []
    message_text = str(message or "").lower()

    base_weights = {
        "internal_rule": 4.0,
        "official": 3.0,
        "faq": 2.0,
        "kms": 1.0,
        "other": 0.0,
    }
    base_score = base_weights.get(source_type, 0.0)
    score += base_score
    if base_score:
        reasons.append(f"base:{source_type}+{base_score}")

    section_title = str(doc.metadata.get("section_title", "")).lower()
    parent_section_title = str(doc.metadata.get("parent_section_title", "")).lower()
    doc_title = str(doc.metadata.get("doc_title", "")).lower()
    page_content = str(doc.page_content or "").lower()
    joined_titles = " ".join([section_title, parent_section_title, doc_title])

    if profile["rule_score"] >= 2:
        if source_type == "internal_rule":
            score += 3.0
            reasons.append("rule_bias:internal_rule+3.0")
        elif source_type == "official":
            score += 1.5
            reasons.append("rule_bias:official+1.5")

    if profile["system_score"] >= 1 and source_type == "kms":
        score += 3.0
        reasons.append("system_bias:kms+3.0")

    if profile["case_score"] >= 1:
        if source_type == "faq":
            score += 1.5
            reasons.append("case_bias:faq+1.5")
        elif source_type == "kms":
            score += 2.0
            reasons.append("case_bias:kms+2.0")

    if profile["official_score"] >= 1 and source_type == "official":
        score += 2.5
        reasons.append("official_bias:official+2.5")

    if profile["is_compare"] and source_type in {"internal_rule", "official"}:
        score += 1.8
        reasons.append("compare_bias:core_source+1.8")

    for normalized_property in profile["normalized_properties"]:
        if normalized_property.lower() in section_title:
            score += 4.0
            reasons.append(f"property_title:{normalized_property}+4.0")
        elif normalized_property.lower() in joined_titles:
            score += 2.5
            reasons.append(f"property_parent:{normalized_property}+2.5")
        elif normalized_property.lower() in page_content:
            score += 1.0
            reasons.append(f"property_body:{normalized_property}+1.0")

    matched_products = 0
    for token in profile["product_hits"]:
        lower_token = token.lower()
        if lower_token in joined_titles:
            matched_products += 1
    if matched_products:
        add_score = matched_products * 2.5
        score += add_score
        reasons.append(f"product_match+{add_score}")

    for institution in profile["institutions"]:
        if institution in detect_doc_institutions(doc):
            score += 1.8
            reasons.append(f"institution:{institution}+1.8")

    if any(keyword in message_text for keyword in ["억원", "만원", "금액", "최대"]):
        if any(keyword in page_content for keyword in ["억원", "만원", "금액", "최대"]):
            score += 0.8
            reasons.append("amount_signal+0.8")

    if str(doc.metadata.get("parent_section_no", "")).strip():
        score += 0.2
        reasons.append("parent_section_hint+0.2")

    rank_bias = max(0, 0.3 - (idx * 0.02))
    score += rank_bias
    reasons.append(f"base_rank_bias+{rank_bias:.2f}")

    return {
        "doc": doc,
        "idx": idx,
        "score": round(score, 4),
        "source_type": source_type,
        "institutions": detect_doc_institutions(doc),
        "reasons": reasons,
    }


def rerank_documents(final_docs, message):
    # [CHANGED ACCURACY] Rerank now stores rich debug traces.
    profile = infer_question_profile(message)
    scored_items = [_score_rerank_item(doc, idx, message, profile) for idx, doc in enumerate(final_docs)]
    scored_items.sort(key=lambda item: (-item["score"], item["idx"]))
    rerank_documents.last_debug = scored_items
    return [item["doc"] for item in scored_items]


def summarize_doc_candidate(doc, fallback_rank=1, bm25_rank=0, vector_rank=0, vector_distance=None, rerank_item=None):
    # [NEW ACCURACY] Uniform candidate summary for diagnostics.
    return {
        "display": format_doc_display(doc, fallback_rank),
        "source_type": classify_source_type(doc),
        "section_no": str(doc.metadata.get("section_no", "")),
        "section_title": str(doc.metadata.get("section_title", "")),
        "parent_section_title": str(doc.metadata.get("parent_section_title", "")),
        "bm25_rank": bm25_rank,
        "vector_rank": vector_rank,
        "vector_score": round(1 / (1 + vector_distance), 4) if vector_distance is not None else 0.0,
        "rerank_score": rerank_item["score"] if rerank_item else 0.0,
        "rerank_reason": " | ".join(rerank_item["reasons"]) if rerank_item else "",
    }


def select_context_documents(reranked_docs, message, final_limit=ACC_FINAL_CONTEXT_K):
    # [NEW ACCURACY] Final context ordering is not identical to raw rerank order.
    profile = infer_question_profile(message)
    selected = []
    selected_keys = set()

    def try_add(doc):
        doc_key = build_doc_key(doc, len(selected) + 1)
        if doc_key in selected_keys or len(selected) >= final_limit:
            return False
        selected.append(doc)
        selected_keys.add(doc_key)
        return True

    if profile["is_compare"]:
        for institution in profile["institutions"]:
            for preferred_source in ["internal_rule", "official", "faq", "kms"]:
                for doc in reranked_docs:
                    if classify_source_type(doc) != preferred_source:
                        continue
                    if institution in detect_doc_institutions(doc):
                        if try_add(doc):
                            break
                if len(selected) >= final_limit:
                    break

    for preferred_source in ["internal_rule", "official", "faq", "kms", "other"]:
        for doc in reranked_docs:
            if classify_source_type(doc) == preferred_source:
                try_add(doc)
            if len(selected) >= final_limit:
                break
        if len(selected) >= final_limit:
            break

    return selected


def prepare_hybrid_retriever(paths=["./kms_output_md", "./faq_output_md", "./official_md", "./내규소수2_md "]):
    # [CHANGED ACCURACY] Wider candidate retrieval, final context capped later.
    all_splits = []

    kms_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
    faq_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)
    rule_overflow_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=200)

    try:
        for path in paths:
            if not os.path.exists(path):
                continue

            print(f"Loading and indexing: {path}")
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
                if is_rule_path and is_internal_rule_doc(doc):
                    if len(doc.page_content) <= 3000:
                        doc.metadata["chunk_id"] = 1
                        all_splits.append(doc)
                    else:
                        file_splits = rule_overflow_splitter.split_documents([doc])
                        for i, split in enumerate(file_splits):
                            split.metadata["chunk_id"] = i + 1
                            all_splits.append(split)
                else:
                    current_splitter = faq_splitter if is_faq_path else kms_splitter
                    file_splits = current_splitter.split_documents([doc])
                    for i, split in enumerate(file_splits):
                        split.metadata["chunk_id"] = i + 1
                        all_splits.append(split)

        print(f"Embedding total chunks: {len(all_splits)}")
        remote_embeddings = RemoteBgeEmbeddings(base_url=EMBED_BASE_URL, model_name=EMBED_MODEL)
        vectorstore = FAISS.from_documents(all_splits, remote_embeddings)
        faiss_retriever = vectorstore.as_retriever(search_kwargs={"k": ACC_HYBRID_K})

        print("Building BM25 index")
        bm25_retriever = BM25Retriever.from_documents(all_splits)
        bm25_retriever.k = ACC_BM25_K

        hybrid_retriever = EnsembleRetriever(
            retrievers=[bm25_retriever, faiss_retriever],
            weights=[0.3, 0.7],
        )
        print(
            f"Hybrid retriever ready: bm25_k={ACC_BM25_K}, vector_k={ACC_VECTOR_K}, "
            f"hybrid_k={ACC_HYBRID_K}, final_context_k={ACC_FINAL_CONTEXT_K}"
        )
        return hybrid_retriever

    except Exception as e:
        print(f"Retriever build error: {str(e)}")
        return None


# [NEW ACCURACY] Rebuild retriever with the new candidate breadth settings.
hybrid_retriever = prepare_hybrid_retriever()

###################
'''
3. Agent 호출(version 03)
- gradio 기반 chat ui
- 답변에 대한 긍/부정 추출
- 세션 구분
'''
###################


### Agnet(v3)
import os
import urllib3
import ipywidgets as widgets
from IPython.display import display
from openai import OpenAI
import gradio as gr

# gr.__version__ #'6.9.0'
# [1] 클라이언트 및 환경 설정
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
client = OpenAI(
    base_url="샘플url이나 이 부분은 제외",
    api_key="dummy"
)
MODEL = "/models"

import pandas as pd
import os
from datetime import datetime
import gradio as gr

FEEDBACK_FILE = "timo_ver0.4_hana_on_feedback_log.csv"
DIAGNOSTIC_LOG_FILE = "timo_ver0.4_hana_on_diagnostic_log.jsonl"  # [NEW ACCURACY]
# [2] 피드백 저장 함수 (상세 로그 반영)
def handle_like(data: gr.LikeData, history, *args):
    try:
        row_idx = data.index[0] if isinstance(data.index, (list, tuple)) else data.index
        ai_response = history[row_idx]['content']
        user_question = history[row_idx-1]['content'] if row_idx > 0 else "N/A"

        new_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "context_turns": getattr(hana_on_predict, 'last_turns', 0), # 맥락 사용량 기록
            "original_question": user_question,
            "searched_query": getattr(hana_on_predict, 'last_query', "N/A"),
            "ai_response": ai_response,
            "feedback": "LIKE" if data.liked else "DISLIKE",
            "ref_documents": getattr(hana_on_predict, 'last_docs', "N/A"),
            "search_reasons": getattr(hana_on_predict, 'last_reasons', "N/A"),
            "vector_similarities": getattr(hana_on_predict, 'last_v_scores', "N/A"),
            "bm25_ranks": getattr(hana_on_predict, 'last_b_ranks', "N/A"),
            "rrf_final_scores": getattr(hana_on_predict, 'last_final_scores', "N/A")
        }

        pd.DataFrame([new_entry]).to_csv(
            FEEDBACK_FILE, mode="a", header=not os.path.exists(FEEDBACK_FILE),
            index=False, encoding='utf-8-sig'
        )
        print(f"📊 [RRF Log] context_turns={new_entry['context_turns']} 저장 완료")

    except Exception as e:
        print(f"⚠️ 로그 저장 오류: {str(e)}")

# 프롬프트
# ==========================================
# [프롬프트 설정 영역] - 전산 화면 코드 대응 버전
# ==========================================

# 1. 질문 재구성(Contextualize) 프롬프트
PROMPT_RE_QUERY = """이전 대화와 현재 질문을 결합하여, 하나은행 전세 대출 규정, 사례 및 전산 가이드를 찾기 위한 최적의 검색어를 생성하세요.

[검색어 생성 규칙]
1. 질문에 4자리 숫자(예: 4400, 4313 등)가 포함된 경우, 이는 단말기 전산 화면 번호이므로 검색어에 해당 숫자를 반드시 포함하세요.
2. 규정 해석이나 사례 문의인 경우 핵심 키워드(예: 소득증빙, 무소득자, 타발송금 수수료 등)를 추출하여 독립적인 질문 형태로 만드세요.

[이전 대화]
{recent_context}

[현재 질문]
{message}

[검색용 질문]:"""

# 2. AI 비서 시스템 지침 (Persona)
PROMPT_SYSTEM_ROLE = """당신은 하나은행 직원의 업무를 돕는 '사내 규정 및 사례 안내 AI 비서'입니다.

[핵심 원칙: 규정 및 사례 우선]
- 모든 답변의 우선순위는 '정확한 규정'과 '실제 처리 사례' 안내에 있습니다.
- 답변 시 언급하는 모든 문서 번호, 수치, 규정 내용은 반드시 제공된 [규정 및 참조 게시물] 내에 존재하는 것이어야 합니다. 외부 지식을 사용해 문서 번호를 임의로 생성하지 마십시오.
- 전산 조작법은 제공된 자료에 명시되어 있는 경우에만 보조적으로 설명하며, 관련 내용이 없다면 굳이 언급하거나 안내하지 마십시오.

[중요1: 4자리 숫자 인지]
- 질문이나 답변 과정에서 등장하는 **4자리 숫자(예: 4400, 4313, #4313 등)**는 하나은행의 **창구 단말기 전산 화면 번호**입니다.
- 이 숫자가 언급될 때만 "단말기 [번호] 화면" 혹은 "[번호] 전산"임을 인지하여 가이드하십시오. (금액, 날짜 등으로 오해하지 마십시오.)

[중요2: 과거 자료 식별 (엄격 적용)]
- 제공된 [규정 및 참조 게시물]의 ##[문서정보] 섹션 내에 **"본 자료는 구.외환 전자상담메뉴얼에서 이관된 자료입니다 (2015년 말 기준)"**이라는 문구가 **토씨 하나 틀리지 않고 그대로 적혀 있는 경우에만** 해당 문서 하단에 과거 자료임을 안내하십시오.
- 해당 문구가 명시되어 있지 않다면 절대로 임의로 과거 자료라고 추측하거나 관련 문구를 추가하지 마십시오.

[답변 가이드라인]
1. 반드시 제공된 [규정 및 참조 게시물] 범위 안에서만 사실에 기반하여 답변하세요.
2. 규정에 없는 내용은 "확인 가능한 규정 및 자료 범위를 벗어난 사항"이라고 명확히 안내하세요.
3. 직원이 업무 중 빠르게 읽을 수 있도록 항목별(Bullet point)로 간결하게 구성하세요.
4. 전산 조작 단계가 포함된 경우에만 번호(1, 2, 3...)를 붙여 기술하세요."""


# 3. 최종 RAG 답변 프롬프트 (Context 주입)
PROMPT_RAG_USER = """아래 [규정 및 전산 가이드]를 참고하여 사용자 질문에 답하세요.

[답변 구성 방식]
- 관련 규정과 사례를 중심으로 명확하게 설명하세요.
- 만약 자료 내에 전산 화면 번호와 조작법이 포함되어 있다면, 직원이 해당 화면에서 무엇을 처리해야 하는지 직관적으로 덧붙여 주십시오.
- **만약 제공된 자료에 전산 조작 관련 내용이 없다면, '전산 처리' 항목을 아예 만들지 말고 언급도 하지 마십시오.**

[규정 및 참조 게시물]ㄱ
{context}

[사용자 질문]
{message}

---
가독성 있게 정리하여 답변하고, 마지막에 '참고 문헌: {src}'를 기재하세요."""


###################
'''
3. RAG 하이브리드 인풋 RAG
'''
###################


# [2] Hana_on Predict 함수 (SyntaxWarning 해결 및 청크 식별자 반영)
# [NEW ACCURACY] ==========================================
# hana_on_predict override with diagnostics and staged selection
# =========================================================
def save_diagnostic_log(record):
    # [NEW ACCURACY] Separate structured diagnostics from feedback CSV.
    try:
        with open(DIAGNOSTIC_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as diag_err:
        print(f"[diagnostic-log-error] {diag_err}")


def hana_on_predict(message, history, context_turns=0):
    try:
        search_query = message

        if context_turns > 0 and history and len(history) > 0:
            recent_context = ""
            for msg in history[-context_turns:]:
                role = "Q" if msg["role"] == "user" else "A"
                recent_context += f"{role}: {msg['content'][:50]}\n"

            re_query_res = client.chat.completions.create(
                model=MODEL,
                messages=[{
                    "role": "user",
                    "content": PROMPT_RE_QUERY.format(recent_context=recent_context, message=message)
                }],
                temperature=0,
            )
            search_query = re_query_res.choices[0].message.content

        question_profile = infer_question_profile(message)
        bm25_retriever = hybrid_retriever.retrievers[0]
        faiss_retriever = hybrid_retriever.retrievers[1]

        # [CHANGED ACCURACY] Retrieval breadth is larger than final context size.
        bm25_candidates = bm25_retriever.invoke(search_query)
        vector_candidates = faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=ACC_VECTOR_K)
        hybrid_candidates = hybrid_retriever.invoke(search_query)
        reranked_candidates = rerank_documents(hybrid_candidates, message)
        final_docs = select_context_documents(reranked_candidates, message, ACC_FINAL_CONTEXT_K)

        bm25_map = {build_doc_key(d, i + 1): i + 1 for i, d in enumerate(bm25_candidates)}
        vector_map = {build_doc_key(d[0], i + 1): (i + 1, d[1]) for i, d in enumerate(vector_candidates)}
        rerank_debug_items = getattr(rerank_documents, "last_debug", [])
        rerank_debug_map = {
            build_doc_key(item["doc"], item["idx"] + 1): item
            for item in rerank_debug_items
        }

        analysis_results = []
        context_parts = []

        for i, d in enumerate(final_docs):
            doc_key = build_doc_key(d, i + 1)
            chunk_display = format_doc_display(d, i + 1)

            in_bm25 = doc_key in bm25_map
            in_vector = doc_key in vector_map
            rerank_item = rerank_debug_map.get(doc_key)

            v_score = round(1 / (1 + vector_map[doc_key][1]), 4) if in_vector else 0.0
            b_rank = bm25_map[doc_key] if in_bm25 else 0
            final_score = rerank_item["score"] if rerank_item else round(1 / (60 + (i + 1)), 6)
            reason = " | ".join(rerank_item["reasons"]) if rerank_item else (
                "Hybrid" if in_bm25 and in_vector else ("Keyword-Only" if in_bm25 else "Vector-Only")
            )

            analysis_results.append({
                "display": chunk_display,
                "reason": reason,
                "b_rank": b_rank,
                "v_sim": v_score,
                "final": final_score,
            })
            context_parts.append(f"--- [참조: {chunk_display}] ---\n{d.page_content}")

        diagnostic_record = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "question": message,
            "search_query": search_query,
            "question_profile": question_profile,
            "bm25_top": [
                summarize_doc_candidate(
                    d,
                    fallback_rank=i + 1,
                    bm25_rank=i + 1,
                    rerank_item=rerank_debug_map.get(build_doc_key(d, i + 1)),
                )
                for i, d in enumerate(bm25_candidates)
            ],
            "vector_top": [
                summarize_doc_candidate(
                    d[0],
                    fallback_rank=i + 1,
                    vector_rank=i + 1,
                    vector_distance=d[1],
                    rerank_item=rerank_debug_map.get(build_doc_key(d[0], i + 1)),
                )
                for i, d in enumerate(vector_candidates)
            ],
            "hybrid_before_rerank": [
                summarize_doc_candidate(
                    d,
                    fallback_rank=i + 1,
                    bm25_rank=bm25_map.get(build_doc_key(d, i + 1), 0),
                    vector_rank=vector_map.get(build_doc_key(d, i + 1), (0, None))[0],
                    vector_distance=vector_map.get(build_doc_key(d, i + 1), (0, None))[1],
                )
                for i, d in enumerate(hybrid_candidates)
            ],
            "rerank_after": [
                summarize_doc_candidate(
                    item["doc"],
                    fallback_rank=i + 1,
                    bm25_rank=bm25_map.get(build_doc_key(item["doc"], item["idx"] + 1), 0),
                    vector_rank=vector_map.get(build_doc_key(item["doc"], item["idx"] + 1), (0, None))[0],
                    vector_distance=vector_map.get(build_doc_key(item["doc"], item["idx"] + 1), (0, None))[1],
                    rerank_item=item,
                )
                for i, item in enumerate(rerank_debug_items)
            ],
            "final_context": [
                summarize_doc_candidate(
                    d,
                    fallback_rank=i + 1,
                    bm25_rank=bm25_map.get(build_doc_key(d, i + 1), 0),
                    vector_rank=vector_map.get(build_doc_key(d, i + 1), (0, None))[0],
                    vector_distance=vector_map.get(build_doc_key(d, i + 1), (0, None))[1],
                    rerank_item=rerank_debug_map.get(build_doc_key(d, i + 1)),
                )
                for i, d in enumerate(final_docs)
            ],
        }
        save_diagnostic_log(diagnostic_record)

        hana_on_predict.last_turns = context_turns
        hana_on_predict.last_query = search_query
        hana_on_predict.last_docs = "|".join([r["display"] for r in analysis_results])
        hana_on_predict.last_reasons = "|".join([r["reason"] for r in analysis_results])
        hana_on_predict.last_v_scores = "|".join([str(r["v_sim"]) for r in analysis_results])
        hana_on_predict.last_b_ranks = "|".join([str(r["b_rank"]) for r in analysis_results])
        hana_on_predict.last_final_scores = "|".join([str(r["final"]) for r in analysis_results])
        hana_on_predict.last_diagnostic_path = DIAGNOSTIC_LOG_FILE

        context_text = "\n\n".join(context_parts)
        src_text = ", ".join([r["display"] for r in analysis_results])

        res = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": PROMPT_SYSTEM_ROLE},
                {"role": "user", "content": PROMPT_RAG_USER.format(context=context_text, message=message, src=src_text)}
            ],
            temperature=0,
        )
        return res.choices[0].message.content

    except Exception as e:
        return f"⚠️ Hana_on 시스템 오류: {str(e)}"


############################
'''
4. Chat UI
'''
############################

import gradio as gr
import pandas as pd
import os
from datetime import datetime
from openai import OpenAI
import urllib3

# [4] 색상 정의
HANA_TEAL = "#008485"
HANA_TEAL_HOVER = "#006d6e"
HANA_TEAL_SOFT = "#e7f4f4"
TEXT_BLACK = "#111111"
BORDER_SOFT = "#cfe3e3"
BG_COLOR = "#fdfdfb"

# [5] Soft 테마 유지 + primary_hue만 teal로 변경
# .set()은 최소한만 사용
theme = gr.themes.Soft(primary_hue="teal").set(
    loader_color=HANA_TEAL,
    slider_color=HANA_TEAL
)

# [6] CSS
custom_css = f"""
.gradio-container {{
    background-color: {BG_COLOR} !important;
}}

/* 상단 제목/부제는 검은색 유지 */
#hana-title h1,
#hana-subtitle p {{
    color: {TEXT_BLACK} !important;
}}

/* 기본 링크/강조 */
.gradio-container a,
.gradio-container a:visited,
.gradio-container strong {{
    color: {HANA_TEAL} !important;
}}

/* Primary 버튼 */
.gradio-container button.primary,
.gradio-container button[variant="primary"] {{
    background-color: {HANA_TEAL} !important;
    border: 1px solid {HANA_TEAL} !important;
    color: #ffffff !important;
}}

.gradio-container button.primary:hover,
.gradio-container button[variant="primary"]:hover {{
    background-color: {HANA_TEAL_HOVER} !important;
    border-color: {HANA_TEAL_HOVER} !important;
}}

/* 포커스 */
.gradio-container textarea:focus,
.gradio-container input:focus,
.gradio-container button:focus,
.gradio-container [role="button"]:focus {{
    border-color: {HANA_TEAL} !important;
    box-shadow: 0 0 0 1px {HANA_TEAL} !important;
    outline: none !important;
}}

/* 입력창 */
.gradio-container textarea,
.gradio-container input {{
    border-color: {BORDER_SOFT} !important;
}}

/* 챗봇 블록 라벨 */
#hana-chatbot [data-testid="block-label"],
#hana-chatbot label {{
    color: {HANA_TEAL} !important;
    font-weight: 700 !important;
}}

/* 표 스타일 */
#hana-chatbot table {{
    border-collapse: collapse !important;
    border-color: {BORDER_SOFT} !important;
}}

#hana-chatbot th {{
    background-color: {HANA_TEAL} !important;
    color: #ffffff !important;
    border: 1px solid {BORDER_SOFT} !important;
}}

#hana-chatbot td {{
    border: 1px solid {BORDER_SOFT} !important;
}}

/* 체크/라디오 */
.gradio-container input[type="checkbox"]:checked,
.gradio-container input[type="radio"]:checked {{
    accent-color: {HANA_TEAL} !important;
}}

/* 아이콘, 화살표, 복사, 전송, 액션 아이콘 */
.gradio-container svg,
.gradio-container svg path,
.gradio-container svg rect,
.gradio-container svg circle,
.gradio-container svg polygon,
.gradio-container svg line,
.gradio-container svg polyline {{
    fill: {HANA_TEAL} !important;
    stroke: {HANA_TEAL} !important;
}}

/* 로딩/스피너 */
.gradio-container [class*="loading"],
.gradio-container [class*="spinner"],
.gradio-container .generating {{
    color: {HANA_TEAL} !important;
    border-color: {HANA_TEAL} !important;
}}

/* 히스토리/패널 선택 강조 */
.gradio-container [aria-selected="true"] {{
    border-color: {HANA_TEAL} !important;
}}

/* hover 계열 */
.gradio-container [role="button"]:hover,
.gradio-container button:hover {{
    border-color: {HANA_TEAL} !important;
}}

/* 텍스트 선택 */
::selection {{
    background: {HANA_TEAL_SOFT};
}}
"""
# # [7] UI 구성
# with gr.Blocks(theme=theme, css=custom_css) as demo:
#     gr.Markdown(
#         "<h1 style='text-align: center; margin-bottom: 0; color: #111111;'>🏦 Hana_on</h1>",
#         elem_id="hana-title"
#     )
#     gr.Markdown(
#         "<p style='text-align: center; color: #111111; font-weight: 600;'>전세대출 Q&A 전문 Agent</p>",
#         elem_id="hana-subtitle"
#     )

#     chatbot_ui = gr.Chatbot(
#         label="Hana_on 대화창",
#         height=550,
#         buttons=["copy"],
#         elem_id="hana-chatbot"
#     )

#     gr.ChatInterface(
#         fn=hana_on_predict,
#         chatbot=chatbot_ui,
#         submit_btn="질문하기",
#         save_history=True
#     )

#     chatbot_ui.like(handle_like, None, None)

# [7] UI 구성
with gr.Blocks(theme=theme, css=custom_css) as demo:
    gr.Markdown(
        "<h1 style='text-align: center; margin-bottom: 0; color: #111111;'>🏦 Hana_on</h1>",
        elem_id="hana-title"
    )
    gr.Markdown(
        "<p style='text-align: center; color: #111111; font-weight: 600;'>전세대출 Q&A 전문 Agent</p>",
        elem_id="hana-subtitle"
    )

    with gr.Row():
        # 맥락 반영 개수를 조절하는 슬라이더 (0~5)
        turns_slider = gr.Slider(
            # visible = False,
            minimum=0, maximum=5, step=1, value=0,
            label="이전 대화 맥락 반영 개수 (0: 단일 질문)",
            info="이전 대화이력을 몇번의 대화이력을 포함시킬까요?"
           
        )

    chatbot_ui = gr.Chatbot(
        label="Hana_on 대화창",
        height=550,
        buttons=["copy"],
        elem_id="hana-chatbot"
    )

    # ChatInterface 설정 시 additional_inputs에 슬라이더 전달
    gr.ChatInterface(
        fn=hana_on_predict,
        chatbot=chatbot_ui,
        additional_inputs= [turns_slider] ,# 멀티턴 제외
        # type="messages",
        save_history=True,
        submit_btn="질문하기"
    )

    chatbot_ui.like(handle_like, [chatbot_ui], None)
   
# [8] 실행
port = 7867
root_path = "/bp20260203038-1-0-n03-ide/proxy/{}".format(port)

demo.launch(
    inline=False,
    server_name="0.0.0.0",
    server_port=port,
    root_path=root_path,
    share=False
)
# NO 링크 질문 정답 AI답변 의견 AI답변2 의견
print(f"http://10.84.115.140:43002{root_path}/")
