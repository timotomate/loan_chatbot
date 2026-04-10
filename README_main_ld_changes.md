# main.py 변경 설명서

이 문서는 `main.py`가 기존 구조에서 어떻게 변경되었는지, 특히 `내규_md`의 LD 분리 파일(`10.1.md`, `10.1.1.md` 등)을 반영하기 위해 어떤 수정이 들어갔는지를 설명한다.

## 1. 변경 배경

기존 `main.py`는 아래 방식으로 문서를 처리했다.

- `faq_output_md`만 `chunk_size=2000`
- `kms_output_md`, `official_md`, `내규_md`는 모두 `chunk_size=800`

하지만 현재 `내규_md`에는 더 이상 통파일 LD 문서가 아니라, 아래와 같이 이미 의미 단위로 분리된 파일이 저장된다.

- `10.1.md`
- `10.2.md`
- `10.1.1.md`
- `7.1.1.md`

이 상태에서 `내규_md`까지 다시 800자로 청킹하면, 조항 단위로 잘 정리된 구조가 다시 깨져 검색 품질이 떨어질 수 있다.

그래서 이번 수정의 핵심 목표는 다음 2가지다.

1. `내규_md`는 기본적으로 재청킹하지 않는다.
2. 내규 검색 결과 표시는 `파일명#청크번호` 대신 `조항번호 + 제목` 중심으로 바꾼다.

## 2. 새로 추가된 전용 유틸 함수

`main.py` 상단에 아래 함수들이 추가되었다.

- `parse_simple_front_matter()`
- `is_internal_rule_doc()`
- `enrich_internal_rule_doc()`
- `build_doc_key()`
- `format_doc_display()`

이 함수들은 `# [ADDED] 내규_md 전용 처리 유틸 시작` 주석 아래에 들어 있다.

### 2-1. `parse_simple_front_matter()`

역할:

- `split_ld_rules_to_md.py`
- `split_ld_rules_to_md_level2.py`

가 생성한 md 파일의 YAML front matter를 읽는다.

예:

```yaml
---
doc_type: "internal_rule"
doc_code: "LD10"
section_no: "10.3"
section_title: "상품명 및 상품코드"
---
```

이 정보를 파싱해서 `metadata`로 옮길 수 있게 만든 함수다.

### 2-2. `is_internal_rule_doc()`

역할:

- 현재 문서가 `내규_md` 문서인지 판별한다.
- 파일 경로에 `내규_md`가 들어 있는지 확인한다.
- 파일명이 `10.1.md`, `10.1.1.md` 같은 형식인지도 같이 본다.

### 2-3. `enrich_internal_rule_doc()`

역할:

- 내규 파일의 front matter를 `doc.metadata`에 넣는다.
- 본문에서는 front matter를 제거한다.
- `doc_type = internal_rule` 값을 보정한다.

즉, 이제 LLM에 들어가는 본문은 더 깔끔해지고, 조항번호/제목은 메타데이터로 별도 보관된다.

### 2-4. `build_doc_key()`

역할:

- 검색 기여도 분석용 키를 만든다.
- 기존에는 `파일명`만 key로 사용했지만, 지금은 `source + chunk_id` 기준으로 바뀌었다.

기존 방식보다 충돌 가능성이 줄어든다.

### 2-5. `format_doc_display()`

역할:

- 검색 결과에 표시할 문서 라벨을 만든다.

표시 규칙:

- 내규 문서면: `LD10 10.3 상품명 및 상품코드`
- 그 외 문서면: 기존처럼 `파일명#청크번호`

즉, KMS/FAQ 디버깅 가독성은 유지하면서 내규는 사람 친화적으로 바뀐다.

## 3. `prepare_hybrid_retriever()`에서 바뀐 점

변경 위치:

- `prepare_hybrid_retriever()`

### 기존 방식

기존에는 아래처럼 FAQ만 예외 처리하고 나머지는 모두 800자 청킹이었다.

```python
current_splitter = faq_splitter if "faq" in path.lower() else kms_splitter
```

### 변경 방식

이제는 경로별로 다음과 같이 다르게 처리한다.

- `faq_output_md`: `2000`자 청킹 유지
- `kms_output_md`, `official_md`: `800`자 청킹 유지
- `내규_md`: 기본적으로 무청킹
- 단, 내규 파일이 너무 길면 `3000`자 기준으로 최소한만 분할

추가된 코드 특징:

- `rule_overflow_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=200)`
- `documents = [enrich_internal_rule_doc(doc) for doc in documents]`
- 내규 문서면 `chunk_id = 1`만 부여하고 그대로 `all_splits`에 넣음

즉, 이미 조항 단위로 쪼개둔 `내규_md`는 구조를 최대한 유지한 채 검색 인덱스에 들어간다.

## 4. `hana_on_predict()`에서 바뀐 점

변경 위치:

- 검색 분석용 맵 생성 부분
- 최종 `chunk_display` 생성 부분

### 4-1. 검색 분석용 key 변경

기존:

```python
bm25_map = {os.path.basename(d.metadata['source']): i+1 for i, d in enumerate(bm25_candidates)}
vector_map = {os.path.basename(d[0].metadata['source']): (i+1, d[1]) for i, d in enumerate(vector_candidates)}
```

변경 후:

```python
bm25_map = {build_doc_key(d, i + 1): i + 1 for i, d in enumerate(bm25_candidates)}
vector_map = {build_doc_key(d[0], i + 1): (i + 1, d[1]) for i, d in enumerate(vector_candidates)}
```

의미:

- 단순 파일명 기준이 아니라 `source + chunk_id` 기준으로 기여도를 추적한다.

### 4-2. 출처 표시 방식 변경

기존:

```python
chunk_display = f"{file_name}#{chunk_num}"
```

변경 후:

```python
chunk_display = format_doc_display(d, i + 1)
```

의미:

- 내규 문서: `LD10 10.3 상품명 및 상품코드`
- 나머지 문서: `파일명#청크번호`

즉, 디버깅 편의성과 내규 가독성을 둘 다 잡는 방향으로 바뀌었다.

## 5. 실제 기대 효과

이번 수정으로 기대하는 효과는 아래와 같다.

- 내규 조항 구조가 검색 전에 깨지지 않는다.
- 질문과 맞는 조항이 더 정확하게 매칭될 가능성이 높아진다.
- 검색 출처가 `10.3.md#1`보다 훨씬 읽기 쉬워진다.
- 피드백 로그에도 내규 문서가 더 이해하기 쉬운 이름으로 남는다.

## 6. 코드에서 변경 지점을 찾는 방법

`main.py`에서 아래 표시를 검색하면 된다.

- `# [ADDED]`
- `# [CHANGED]`

특히 아래 구간을 보면 된다.

- `# [ADDED] 내규_md 전용 처리 유틸 시작`
- `# [CHANGED] 내규_md 문서는 front matter를 metadata로 이동`
- `# [CHANGED] 내규 데이터는 이미 10.1 / 10.1.1 등 의미 단위로 분리됨`
- `# [CHANGED] 분석용 맵: 파일명만이 아니라 source + chunk_id 기준으로 기여도 판별`
- `# [CHANGED] 내규는 조항번호/제목 중심, 그 외는 파일명#청크번호 유지`

## 7. 주의사항

- 현재 방식은 `내규_md` 안의 md 파일이 `split_ld_rules_to_md.py` 또는 `split_ld_rules_to_md_level2.py`가 만든 형식을 따른다는 전제에 맞춰져 있다.
- front matter 형식이 크게 바뀌면 `parse_simple_front_matter()`도 같이 손봐야 한다.
- 내부망에서 실제 검증할 때는 최소한 아래 2가지는 확인하는 것이 좋다.

1. 내규 질문 시 참고 문헌이 `LD10 10.3 ...` 형태로 잘 나오는지
2. 리트리버 생성 시 `내규:무청킹(장문만 3000)` 로그가 정상 출력되는지

## 8. 요약

이번 변경은 단순 청킹 크기 조정이 아니라, `내규_md`를 별도 문서군으로 취급하도록 검색 파이프라인을 보정한 작업이다.

핵심 요약:

- `내규_md`는 기본 무청킹
- front matter를 metadata로 이동
- 내규 출처는 `조항번호 + 제목` 중심으로 표시
- KMS/FAQ/official 쪽 동작은 기존과 최대한 비슷하게 유지

