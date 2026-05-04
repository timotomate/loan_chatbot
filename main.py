# 01. 검색 DB

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

def prepare_hybrid_retriever(paths=["./kms_output_md", "./faq_output_md", "./official_md","./내규소수2_md"]):
    """
    KMS와 FAQ에 개별 청킹을 적용하고, FAISS(벡터)와 BM25(키워드)를 결합한 하이브리드 리트리버를 반환합니다.
    """
    all_splits = []

    # [규칙 정의] 소스별 서로 다른 스플리터 설정
    kms_splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)
    faq_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)

    try:
        for path in paths:
            if not os.path.exists(path): continue

            print(f"⏳ {path} 로드 및 인덱싱 중...")
            loader = DirectoryLoader(path, glob="*.md", loader_cls=TextLoader,
                                     loader_kwargs={'encoding': 'utf-8', 'autodetect_encoding': True})
            documents = loader.load()
           
            for doc in documents:
                if is_rule_path and is_internal_rule_doc(doc):
                    file_splits = rule_overflow_splitter.split_documents([doc])
                    for i, split in enumerate(file_splits):
                        split.metadata["chunk_id"] = i + 1
                        all_splits.append(split)

        # 3. FAISS 벡터 DB 생성 (의미 검색용)
        print(f"⏳ 총 {len(all_splits)}개 청크 임베딩 중...")
        remote_embeddings = RemoteBgeEmbeddings(base_url=EMBED_BASE_URL, model_name=EMBED_MODEL)
        vectorstore = FAISS.from_documents(all_splits, remote_embeddings)
        faiss_retriever = vectorstore.as_retriever(search_kwargs={"k": 5})

        # 4. BM25 리트리버 생성 (키워드 검색용)
        # 중요: 벡터 DB를 만들 때 사용한 것과 동일한 all_splits를 사용합니다.
        print("⏳ BM25 키워드 인덱싱 중...")
        bm25_retriever = BM25Retriever.from_documents(all_splits)
        bm25_retriever.k = 5

        # 5. 하이브리드 앙상블 리트리버 생성 (RRF 방식 결합)
        # 키워드(BM25)와 의미(FAISS)의 비중을 3:7로 설정
        hybrid_retriever = EnsembleRetriever(
            retrievers=[bm25_retriever, faiss_retriever],
            weights=[0.3, 0.7]
        )

        print("✨ [KMS:800 / FAQ:2000 ] 하이브리드 리트리버 시스템 구축 완료!")
        return hybrid_retriever

    except Exception as e:
        print(f"❌ 상세 오류 발생: {str(e)}")
        return None

# 리트리버 초기화
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

FEEDBACK_FILE = "hana_on_feedback_log.csv"
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
def hana_on_predict(message, history, context_turns=0):
    try:
        search_query = message
       
        # [Step 1] 질문 재구성 (멀티턴 제어) [cite: 131, 132]
        if context_turns > 0 and history and len(history) > 0:
            recent_context = ""
            for msg in history[-context_turns:]:
                role = "Q" if msg["role"] == "user" else "A"
                recent_context += f"{role}: {msg['content'][:50]}\n"

            # 쉼표 누락 방지를 위해 명확하게 구문 작성
            re_query_res = client.chat.completions.create(
                model=MODEL,
                messages=[{
                    "role": "user",
                    "content": PROMPT_RE_QUERY.format(recent_context=recent_context, message=message)
                }],
                temperature=0,  # 뒤에 쉼표를 명시하여 다음 인자와 구분
            )
            search_query = re_query_res.choices[0].message.content

        # [Step 2] 하이브리드 검색 및 분석 [cite: 134]
        bm25_retriever = hybrid_retriever.retrievers[0]
        faiss_retriever = hybrid_retriever.retrievers[1]
       
        bm25_candidates = bm25_retriever.invoke(search_query)
        # FAISS는 유사도 점수 분석을 위해 similarity_search_with_score 사용 [cite: 134]
        vector_candidates = faiss_retriever.vectorstore.similarity_search_with_score(search_query, k=5)

        # [CHANGED] 분석용 맵: 파일명만이 아니라 source + chunk_id 기준으로 기여도 판별
        bm25_map = {build_doc_key(d, i + 1): i + 1 for i, d in enumerate(bm25_candidates)}
        vector_map = {build_doc_key(d[0], i + 1): (i + 1, d[1]) for i, d in enumerate(vector_candidates)}

        # 최종 앙상블 결과 추출 [cite: 135]
        final_docs = hybrid_retriever.invoke(search_query)
        final_docs = rerank_documents(final_docs, message)
       
        analysis_results = []
        context_parts = []
       
        for i, d in enumerate(final_docs):
            # [CHANGED] 내규는 조항번호/제목 중심, 그 외는 파일명#청크번호 유지
            doc_key = build_doc_key(d, i + 1)
            chunk_display = format_doc_display(d, i + 1)
           
            in_bm25 = doc_key in bm25_map
            in_vector = doc_key in vector_map
           
            v_score = round(1 / (1 + vector_map[doc_key][1]), 4) if in_vector else 0.0
            b_rank = bm25_map[doc_key] if in_bm25 else 0
           
            reason = "Hybrid" if in_bm25 and in_vector else ("Keyword-Only" if in_bm25 else "Vector-Only")
            final_score = round(1 / (60 + (i + 1)), 6)

            analysis_results.append({
                "display": chunk_display,
                "reason": reason,
                "b_rank": b_rank,
                "v_sim": v_score,
                "final": final_score
            })
            context_parts.append(f"--- [참조: {chunk_display}] ---\n{d.page_content}")

        # 피드백용 속성 저장 [cite: 140]
        hana_on_predict.last_turns = context_turns
        hana_on_predict.last_query = search_query
        hana_on_predict.last_docs = "|".join([r['display'] for r in analysis_results])
        hana_on_predict.last_reasons = "|".join([r['reason'] for r in analysis_results])
        hana_on_predict.last_v_scores = "|".join([str(r['v_sim']) for r in analysis_results])
        hana_on_predict.last_b_ranks = "|".join([str(r['b_rank']) for r in analysis_results])
        hana_on_predict.last_final_scores = "|".join([str(r['final']) for r in analysis_results])
        hana_on_predict.last_context = final_docs

        # [Step 3] 최종 답변 생성 [cite: 141, 142]
        context_text = "\n\n".join(context_parts)
        src_text = ", ".join([r['display'] for r in analysis_results])
       
        res = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": PROMPT_SYSTEM_ROLE},
                {"role": "user", "content": PROMPT_RAG_USER.format(context=context_text, message=message, src=src_text)}
            ],
            temperature=0, # 구문 경고 방지를 위해 마지막 인자에도 명확한 쉼표 사용 권장
        )
    return res.choices[0].message.content


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