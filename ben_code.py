import pandas as pd
import json
import re
class SplitHybridRetriever:
 """
 KMS / non-KMS 구조를 유지하면서도 상위 코드가 retriever 내부 구현에 직접 의존하지 않도록
 공통 인터페이스를 제공하는 wrapper.
 """

 def __init__(self, kms_bundle, non_kms_bundle, top_ensemble):
  self.kms = kms_bundle
  self.non_kms = non_kms_bundle
  self.top_ensemble = top_ensemble

 def invoke(self, query: str):
  return self.top_ensemble.invoke(query)

 def get_debug_candidates(self, query: str, vector_k: int = 12):
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


def get_retrieval_debug_info(hybrid_retriever, search_query: str, vector_k: int = 12):
 """
 hana_on_predict()가 retriever 내부 구조를 직접 알지 않아도 되도록 해주는 어댑터.

 사용 예시:
 debug_info = get_retrieval_debug_info(hybrid_retriever, search_query, vector_k=12)

 if "bm25_docs" in debug_info:
     bm25_candidates = debug_info["bm25_docs"]
     vector_candidates = debug_info["vector_docs"]
     hybrid_candidates = debug_info["top_hybrid_docs"]
 else:
     kms_candidates = debug_info["kms_docs"]
     non_kms_bm25_candidates = debug_info["non_kms_bm25_docs"]
     non_kms_vector_candidates = debug_info["non_kms_vector_docs"]
     hybrid_candidates = debug_info["top_hybrid_docs"]
 """
 if hasattr(hybrid_retriever, "get_debug_candidates"):
  return hybrid_retriever.get_debug_candidates(search_query, vector_k=vector_k)

 bm25_retriever = hybrid_retriever.retrievers[0]
 faiss_retriever = hybrid_retriever.retrievers[1]
 return {
  "bm25_docs": bm25_retriever.invoke(search_query),
  "vector_docs": faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=vector_k),
  "top_hybrid_docs": hybrid_retriever.invoke(search_query),
 }


import os
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
from openai import OpenAI

try:
 from langchain.embeddings.base import Embeddings
except ImportError:
 Embeddings = object


"""
[중요]
이 파일은 KMS와 비KMS(FAQ / official / 내규)를 분리해서 검색하려는 목적의 코드이다.

기존 main.py / main_accuracy_v4.py의 hana_on_predict()는 retriever 내부 구조를 다음처럼 가정했다.
- retrievers[0] = BM25
- retrievers[1] = FAISS retriever

하지만 이 파일은
- KMS: ParentDocumentRetriever + BM25 앙상블
- 비KMS: flat chunk 기반 BM25 + FAISS 앙상블
- 최종: 위 두 그룹을 다시 앙상블

하는 '중첩 구조'이므로, 기존 hana_on_predict()가 retrievers[1].vectorstore 등에 직접 접근하면
Gradio 요청 시 500 에러가 날 수 있다.

따라서 이 파일을 반영할 때는:
1. prepare_hybrid_retriever() 교체
2. hybrid_retriever 초기화 교체
3. hana_on_predict() 안의 bm25/vector 후보 수집 방식 교체

를 세트로 적용해야 한다.
"""


# --- 원격 임베딩 설정 ---
EMBED_BASE_URL = "http://10.84.116.20:43021/llm/bge-m3/v1"
EMBED_MODEL = "/models"


class RemoteBgeEmbeddings(Embeddings):
 """BGE-M3 ??? ?? ???? ???? LangChain ?? ??? ???."""

 def __init__(self, base_url: str = EMBED_BASE_URL, model_name: str = EMBED_MODEL):
  self.client = OpenAI(base_url=base_url, api_key="dummy")
  self.model = model_name

 def embed_documents(self, texts: list[str]) -> list[list[float]]:
  response = self.client.embeddings.create(model=self.model, input=texts)
  return [data.embedding for data in response.data]

 def embed_query(self, text: str) -> list[float]:
  response = self.client.embeddings.create(model=self.model, input=[text])
  return response.data[0].embedding

def _get_direct_rows(table_soup):
 """테이블의 직계 자식 tr만 반환 (thead/tbody/tfoot 한 단계만 내려감, 중첩 테이블 제외)"""
 rows = []
 for child in table_soup.children:
 if not hasattr(child, 'name'):
 continue
 if child.name == 'tr':
 rows.append(child)
 elif child.name in ('thead', 'tbody', 'tfoot'):
 for subchild in child.children:
 if hasattr(subchild, 'name') and subchild.name == 'tr':
 rows.append(subchild)
 return rows


def convert_html_table_to_md(table_soup):
 """HTML 테이블을 마크다운 테이블로 변환 (중첩 테이블 재귀 처리)"""
 rows = _get_direct_rows(table_soup)
 if not rows:
 return ""

 md_table = []
 nested_md_blocks = []

 for i, row in enumerate(rows):
 cells = row.find_all(['td', 'th'], recursive=False)
 cell_texts = []
 for cell in cells:
 # 셀 내 중첩 테이블을 재귀 변환 후 별도 블록으로 수집하고 셀에서 제거
 for nested in cell.find_all('table'):
 nested_md = convert_html_table_to_md(nested)
 if nested_md:
 nested_md_blocks.append(nested_md)
 nested.decompose()
 text = cell.get_text(separator=' ', strip=True).replace('\n', ' ')
 cell_texts.append(text)

 if not cell_texts:
 continue
 md_table.append("| " + " | ".join(cell_texts) + " |")
 if i == 0:
 md_table.append("| " + " | ".join(['---'] * len(cell_texts)) + " |")

 result_parts = ["\n".join(md_table)] + nested_md_blocks
 return "\n\n".join(part for part in result_parts if part)
 

def extract_service_code(html_str):
 """HTML 내 catalog 경로에서 서비스 코드만 추출"""
 match = re.search(r'catalog="(/[^"]+)/catalog\.xml\.jsp"', html_str)
 if match:
 path = match.group(1).strip('/')
 parts = path.split('/')
 if len(parts) >= 2:
 return parts[1].lower()
 return ""

def parse_kms_html_to_md(html_str, doc_type, gid_path_map):
 """
 마크다운 생성 (벡터 DB 최적화 버전)

 구조:
 --- ← YAML frontmatter (임베딩 제외, metadata로만 사용)
 지식_ID: ...
 분류: ...
 ...
 ---
 # 문서 제목 ← H1 (MarkdownHeaderTextSplitter 기준점)
 본문 내용 ... ← 임베딩될 순수 본문
 """
 try:
 model_pattern = re.compile(r'model = (\{.*?\});', re.DOTALL)
 match = model_pattern.search(html_str)
 if not match:
 return "Error: model 데이터를 찾을 수 없습니다."

 model_data = json.loads(match.group(1))
 gid = str(model_data.get('gid', 'N/A'))
 doc_id = str(model_data.get('id', 'N/A'))

 # 1. 서비스 코드 및 지식 ID 조합
 service_code = extract_service_code(html_str)
 if gid == doc_id:
 display_id = f"{service_code}_{gid}" if service_code else gid
 else:
 display_id = f"{service_code}_{gid}_{doc_id}" if service_code else f"{gid}_{doc_id}"

 # 2. 분류 경로 (Type + GID 조합으로 매핑 테이블 조회)
 path_str = gid_path_map.get((doc_type, gid), "분류 없음")

 title = model_data.get('title', '제목 없음')
 author = model_data.get('author', {}).get('displayName', '미상')
 last_updt_ts = model_data.get('lastUpdt', 0) / 1000
 last_updt = datetime.fromtimestamp(last_updt_ts).strftime('%Y-%m-%d %H:%M:%S') if last_updt_ts > 0 else "N/A"
 is_legacy = model_data.get('trnsSrc') in ["KEB_KB_SVKNW_COUNSEL", "KEB_KB_SVKNW_ONESTOP"]

 # 3. 첨부파일 목록 수집
 attachments = model_data.get('attachments', [])
 att_list = [f"{att.get('filename')} (ID: {att.get('id')})" for att in attachments]

 # --- YAML Frontmatter 구성 (벡터 DB의 metadata 필드로 사용) ---
 # YAML 값에 콜론·따옴표 포함 가능성이 있어 큰따옴표로 감쌈
 frontmatter_lines = [
 "---",
 f'지식_ID: "{display_id}"',
 f'제목: "{title}"',
 f'분류: "{path_str}"',
 f'최종수정일: "{last_updt}"',
 f'작성자: "{author}"',
 f'이관자료: {"true" if is_legacy else "false"}',
 ]
 if att_list:
 frontmatter_lines.append("첨부파일:")
 for att in att_list:
 frontmatter_lines.append(f' - "{att}"')
 frontmatter_lines.append("---")

 # 4. 본문(content) HTML → 순수 텍스트 변환
 content_html = model_data.get('content', '')
 content_soup = BeautifulSoup(content_html, 'lxml')

 for a in content_soup.find_all('a', href=True):
 link_text = a.get_text(strip=True)
 link_url = a['href']
 if link_url.startswith('/'):
 base_url = "https://kms.financial.com"
 link_url = base_url + link_url
 a.replace_with(f" [{link_text}]({link_url}) ")

 # 최상위 테이블만 처리 (중첩 테이블은 convert_html_table_to_md 내부에서 재귀 처리)
 for table in content_soup.find_all('table'):
 if table.find_parent('table'):
 continue
 md_table = convert_html_table_to_md(table)
 table.replace_with(f"\n\n{md_table}\n\n")

 body_text = content_soup.get_text(separator="\n", strip=True)
 body_text = re.sub(r'\n{3,}', '\n\n', body_text)

 # 5. 최종 조합: frontmatter + H1 제목 + 순수 본문
 parts = frontmatter_lines + [f"\n# {title}\n", body_text]
 return "\n".join(parts)

 except Exception as e:
 return f"Error 발생: {str(e)}"

# --- 메인 실행 로직 ---

def process_dataframe(df, output_dir='kms_output_md'):
 if not os.path.exists(output_dir):
 os.makedirs(output_dir)

 # [Step 1] Type별 중복 고려한 매핑 테이블 생성
 print("⏳ Type별 마스터 문서 필터링 및 경로 매핑 중...")
 gid_path_map = {}
 model_pattern = re.compile(r'model = (\{.*?\});', re.DOTALL)
 
 # tab이 NaN인 마스터 행 추출
 master_df = df[df['id'].isna()]
 
 for _, row in master_df.iterrows():
 try:
 doc_type = row['type']
 match = model_pattern.search(row['result'])
 if match:
 data = json.loads(match.group(1))
 curr_gid = str(data.get('gid'))
 
 # 경로 추출 로직 분기
 path_str = "분류 없음"
 if doc_type == 'kms':
 # kms인 경우 folders (리스트)에서 추출
 folders = data.get('folders', [])
 if folders:
 path_str = " > ".join(folders[0].get('path', []))
 else:
 # kms가 아닌 경우 folder (딕셔너리)에서 추출
 folder = data.get('folder', {})
 if folder:
 path_str = " > ".join(folder.get('path', []))
 
 # 중복 방지를 위해 (Type, GID)를 키로 사용
 gid_path_map[(doc_type, curr_gid)] = path_str
 except:
 continue
 
 print(f"✅ 매핑 완료: {len(gid_path_map)}개의 고유 경로 확보")

 # [Step 2] 전체 데이터 변환 및 저장
 print(f"⏳ 총 {len(df)}개 파일 변환 시작...")
 for idx, row in df.iterrows():
 doc_type = row['type'] 
 md_result = parse_kms_html_to_md(row['result'], doc_type, gid_path_map)
 
 # Type별 ID 중복 가능성이 있으므로 파일명에 pk로
 file_path = os.path.join(output_dir, f"{row['pk']}.md")
 with open(file_path, 'w', encoding='utf-8') as f:
 f.write(md_result)
 print(f" - 저장 완료: {file_path} ({idx+1}/{len(df)})")
 
 print("✅ 모든 파일 변환 완료")


# --- 벡터 DB 적재 로직 ---

def _parse_frontmatter(text: str) -> tuple[dict, str]:
 """
 YAML frontmatter(--- 블록)를 파싱합니다.
 외부 패키지 없이 PyYAML(yaml) 또는 정규식으로 처리합니다.

 Returns:
 (metadata dict, frontmatter 제거된 본문 str)
 """
 fm_match = re.match(r'^---\n(.*?)\n---\n?', text, re.DOTALL)
 if not fm_match:
 return {}, text

 fm_text = fm_match.group(1)
 body = text[fm_match.end():]

 # PyYAML 로 파싱 (일반적으로 기본 설치되어 있음)
 try:
 import yaml
 metadata = yaml.safe_load(fm_text) or {}
 return metadata, body
 except ImportError:
 pass

 # PyYAML 없을 경우 정규식으로 직접 파싱
 metadata = {}
 current_list_key = None
 for line in fm_text.splitlines():
 # 리스트 항목 (들여쓰기 + "- ")
 list_match = re.match(r'^\s+- "(.*)"$', line)
 if list_match and current_list_key:
 metadata[current_list_key].append(list_match.group(1))
 continue

 # 리스트 키 선언 (값 없이 콜론으로 끝나는 경우)
 list_key_match = re.match(r'^(\S+):$', line)
 if list_key_match:
 current_list_key = list_key_match.group(1)
 metadata[current_list_key] = []
 continue

 # 일반 키-값 쌍
 kv_match = re.match(r'^([^:]+):\s*"?(.*?)"?$', line)
 if kv_match:
 current_list_key = None
 key = kv_match.group(1).strip()
 val = kv_match.group(2).strip().strip('"')
 metadata[key] = val

 return metadata, body


# --- FAISS + BM25 Ensemble Retriever ---

def _load_parent_docs(md_dir: str) -> list:
 """
 .md 파일들을 헤더 기준으로 분리해 부모 문서 리스트를 반환합니다.
 build/load 양쪽에서 공통으로 사용합니다.
 """
 try:
 from langchain.text_splitter import MarkdownHeaderTextSplitter
 except ImportError:
 raise ImportError("pip install langchain 을 먼저 실행하세요.")

 headers_to_split_on = [("#", "제목"), ("##", "섹션"), ("###", "소섹션")]
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
 meta["파일명"] = md_file.name
 category_str = meta.get("분류", "")
 if category_str and category_str != "분류 없음":
 for depth, part in enumerate(category_str.split(" > "), start=1):
 meta[f"분류_depth{depth}"] = part.strip()
 sections = header_splitter.split_text(body)
 for section in sections:
 section.metadata.update(meta)
 parent_docs.extend(sections)
 except Exception as e:
 print(f" ⚠️ {md_file.name} 처리 실패: {e}")
 return parent_docs


def build_ensemble_retriever(
 md_dir: str = 'kms_output_md',
 embeddings=None,
 faiss_save_path: str = 'faiss_index',
 docstore_save_path: str = 'docstore.pkl',
 parents_save_path: str = 'parent_docs.pkl',
 child_chunk_size: int = 800,
 child_chunk_overlap: int = 100,
 dense_weight: float = 0.5,
 sparse_weight: float = 0.5,
 bm25_k: int = 4,
):
 """
 EnsembleRetriever (FAISS Dense + BM25 Sparse) + ParentDocumentRetriever 구성 및 저장.

 검색 흐름:
 질문
 ├── Dense: FAISS (Child 청크 벡터 검색) → ParentDocumentRetriever → Parent 반환
 └── Sparse: BM25 (Parent 문서 키워드 검색) → Parent 직접 반환
 ↓ RRF(Reciprocal Rank Fusion)으로 결과 병합
 최종 Parent 문서 → LLM

 필요 패키지:
 pip install langchain langchain-community faiss-cpu rank_bm25
 """
 import pickle
 import faiss as faiss_lib
 try:
 from langchain_community.vectorstores import FAISS
 from langchain_community.docstore.in_memory import InMemoryDocstore
 from langchain_community.retrievers import BM25Retriever
 from langchain.storage import InMemoryStore
 from langchain.retrievers import ParentDocumentRetriever, EnsembleRetriever
 from langchain.text_splitter import RecursiveCharacterTextSplitter
 except ImportError:
 raise ImportError("pip install langchain langchain-community faiss-cpu rank_bm25 을 먼저 실행하세요.")

 if embeddings is None:
 raise ValueError("embeddings 모델을 전달해야 합니다. 예) OpenAIEmbeddings()")

 # 1. 부모 문서 로드
 parent_docs = _load_parent_docs(md_dir)
 print(f"✅ 부모 문서 {len(parent_docs)}개 로드 완료")

 # 2. Dense 리트리버: 빈 FAISS + ParentDocumentRetriever
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
 print(f"⏳ Dense 임베딩 및 인덱싱 중...")
 dense_retriever.add_documents(parent_docs)
 print(f"✅ Dense 인덱싱 완료")

 # 3. Sparse 리트리버: BM25 (Parent 문서 전체 대상)
 bm25_retriever = BM25Retriever.from_documents(parent_docs)
 bm25_retriever.k = bm25_k

 # 4. EnsembleRetriever (RRF 병합)
 ensemble_retriever = EnsembleRetriever(
 retrievers=[dense_retriever, bm25_retriever],
 weights=[dense_weight, sparse_weight],
 )

 # 5. 저장
 vectorstore.save_local(faiss_save_path)
 with open(docstore_save_path, 'wb') as f:
 pickle.dump(dict(docstore.store), f)
 with open(parents_save_path, 'wb') as f:
 pickle.dump(parent_docs, f)

 print(f"✅ FAISS 저장: {faiss_save_path}/")
 print(f"✅ docstore 저장: {docstore_save_path}")
 print(f"✅ parent docs 저장: {parents_save_path}")
 return ensemble_retriever


def load_ensemble_retriever(
 embeddings=None,
 faiss_save_path: str = 'faiss_index',
 docstore_save_path: str = 'docstore.pkl',
 parents_save_path: str = 'parent_docs.pkl',
 child_chunk_size: int = 800,
 child_chunk_overlap: int = 100,
 dense_weight: float = 0.5,
 sparse_weight: float = 0.5,
 bm25_k: int = 4,
):
 """
 저장된 인덱스를 불러와 EnsembleRetriever를 복원합니다.

 사용 예:
 retriever = load_ensemble_retriever(embeddings=OpenAIEmbeddings())
 docs = retriever.invoke("에스크로 주계약자 가입 절차 알려줘")
 """
 import pickle
 try:
 from langchain_community.vectorstores import FAISS
 from langchain_community.retrievers import BM25Retriever
 from langchain.storage import InMemoryStore
 from langchain.retrievers import ParentDocumentRetriever, EnsembleRetriever
 from langchain.text_splitter import RecursiveCharacterTextSplitter
 except ImportError:
 raise ImportError("pip install langchain langchain-community faiss-cpu rank_bm25 을 먼저 실행하세요.")

 if embeddings is None:
 raise ValueError("embeddings 모델을 전달해야 합니다.")

 # Dense 리트리버 복원
 vectorstore = FAISS.load_local(
 faiss_save_path,
 embeddings,
 allow_dangerous_deserialization=True,
 )
 with open(docstore_save_path, 'rb') as f:
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

 # Sparse 리트리버 복원 (BM25는 저장 불가 → parent docs로 재구성)
 with open(parents_save_path, 'rb') as f:
 parent_docs = pickle.load(f)
 bm25_retriever = BM25Retriever.from_documents(parent_docs)
 bm25_retriever.k = bm25_k

 # EnsembleRetriever 복원
 ensemble_retriever = EnsembleRetriever(
 retrievers=[dense_retriever, bm25_retriever],
 weights=[dense_weight, sparse_weight],
 )

 print(f"✅ EnsembleRetriever 로드 완료")
 return ensemble_retriever


import os

from langchain_core.embeddings import Embeddings
from openai import OpenAI
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_community.retrievers import BM25Retriever
from langchain.retrievers import EnsembleRetriever

# ver0.2.0


def prepare_hybrid_retriever(
 non_kms_paths=["./faq_output_md", "./official_md", "./내규소수2_md"],
 kms_faiss_path="faiss_index",
 kms_docstore_path="docstore.pkl",
 kms_parents_path="parent_docs.pkl",
):
 """
 KMS / 비KMS 분리 목적은 유지하되, 최종 반환 객체는 SplitHybridRetriever 로 통일합니다.
 이렇게 해야 hana_on_predict() 가 retriever 내부 구현을 직접 파고들지 않아도 됩니다.
 """
 remote_embeddings = RemoteBgeEmbeddings(base_url=EMBED_BASE_URL, model_name=EMBED_MODEL)

 try:
  # ── KMS: 저장된 ParentDocumentRetriever + BM25 앙상블 로드 ──
  print("⏳ KMS 인덱스 로드 중...")
  kms_ensemble = load_ensemble_retriever(
   embeddings=remote_embeddings,
   faiss_save_path=kms_faiss_path,
   docstore_save_path=kms_docstore_path,
   parents_save_path=kms_parents_path,
  )

  # ── 비KMS: FAQ / official / 내규는 flat chunk 유지 ──
  all_splits = []
  kms_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
  faq_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)
  rule_overflow_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=200)

  for path in non_kms_paths:
   if not os.path.exists(path):
    continue

   print(f"⏳ {path} 로드 및 인덱싱 중...")
   loader = DirectoryLoader(
    path,
    glob="*.md",
    loader_cls=TextLoader,
    loader_kwargs={"encoding": "utf-8", "autodetect_encoding": True},
   )
   documents = loader.load()

   enriched_documents = []
   for doc in documents:
    try:
     enriched_documents.append(enrich_internal_rule_doc(doc))
    except Exception:
     enriched_documents.append(doc)

   is_faq_path = "faq" in path.lower()
   is_rule_path = ("내규" in path) or ("rule" in path.lower())

   for doc in enriched_documents:
    if is_rule_path:
     try:
      is_rule_doc = is_internal_rule_doc(doc)
     except Exception:
      is_rule_doc = False
    else:
     is_rule_doc = False

    if is_rule_doc:
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

  if all_splits:
   print(f"⏳ 비KMS {len(all_splits)}개 청크 임베딩 중...")
   vectorstore = FAISS.from_documents(all_splits, remote_embeddings)
   non_kms_faiss = vectorstore.as_retriever(search_kwargs={"k": 5})

   print("⏳ 비KMS BM25 인덱싱 중...")
   non_kms_bm25 = BM25Retriever.from_documents(all_splits)
   non_kms_bm25.k = 5

   non_kms_ensemble = EnsembleRetriever(
    retrievers=[non_kms_bm25, non_kms_faiss],
    weights=[0.3, 0.7],
   )

   top_ensemble = EnsembleRetriever(
    retrievers=[kms_ensemble, non_kms_ensemble],
    weights=[0.5, 0.5],
   )

   hybrid_retriever = SplitHybridRetriever(
    kms_bundle={
     "ensemble": kms_ensemble,
    },
    non_kms_bundle={
     "bm25_retriever": non_kms_bm25,
     "faiss_retriever": non_kms_faiss,
     "ensemble": non_kms_ensemble,
    },
    top_ensemble=top_ensemble,
   )
  else:
   # 비KMS 데이터가 없으면 KMS 앙상블만 top retriever 로 사용
   hybrid_retriever = SplitHybridRetriever(
    kms_bundle={
     "ensemble": kms_ensemble,
    },
    non_kms_bundle={
     "bm25_retriever": None,
     "faiss_retriever": None,
     "ensemble": None,
    },
    top_ensemble=kms_ensemble,
   )

  print("✨ KMS / 비KMS 분리 하이브리드 리트리버 구축 완료!")
  return hybrid_retriever

 except Exception as e:
  print(f"❌ 상세 오류 발생: {str(e)}")
  return None


# 리트리버 초기화
hybrid_retriever = prepare_hybrid_retriever()


"""
[hana_on_predict 교체 가이드 - 그대로 복붙용]

기존 main.py / main_accuracy_v4.py 안의 아래 구간:

    bm25_retriever = hybrid_retriever.retrievers[0]
    faiss_retriever = hybrid_retriever.retrievers[1]
    bm25_candidates = bm25_retriever.invoke(search_query)
    vector_candidates = faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=5)
    final_docs = hybrid_retriever.invoke(search_query)

는 KMS / 비KMS 분리 구조에서 그대로 쓰면 안 됩니다.
반드시 아래 형태로 바꿔야 합니다.

----------------------------------------
debug_info = get_retrieval_debug_info(hybrid_retriever, search_query, vector_k=5)

if "bm25_docs" in debug_info:
    # 기존 flat 구조 fallback
    bm25_candidates = debug_info["bm25_docs"]
    vector_candidates = debug_info["vector_docs"]
    final_docs = debug_info["top_hybrid_docs"]
else:
    # SplitHybridRetriever 구조
    kms_candidates = debug_info["kms_docs"]
    non_kms_bm25_candidates = debug_info["non_kms_bm25_docs"]
    non_kms_vector_candidates = debug_info["non_kms_vector_docs"]
    final_docs = debug_info["top_hybrid_docs"]

    # 기존 로그/분석 코드와 호환되도록 비KMS 쪽을 bm25/vector 기준 후보로 사용
    bm25_candidates = non_kms_bm25_candidates if non_kms_bm25_candidates else kms_candidates
    vector_candidates = non_kms_vector_candidates
----------------------------------------

핵심:
1. 더 이상 hybrid_retriever.retrievers[0/1] 에 직접 접근하지 말 것
2. get_retrieval_debug_info() 를 통해 후보군을 받아올 것
3. final_docs 는 항상 debug_info["top_hybrid_docs"] 를 사용할 것

이 3개를 같이 반영해야 Gradio 500 가능성을 줄일 수 있습니다.
"""
