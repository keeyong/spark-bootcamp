import pandas as pd
import numpy as np
import time
import gc


def generate_large_csv_in_chunks(file_path, chunk_size=1_000_000, total_rows=100_000_000):
    """
    청크 단위로 대용량 CSV 파일을 다수의 작은 파일로 생성하여 저장하는 함수.
    청크는 별도 파일로 저장되며, 모든 파일에는 헤더가 포함된다.
    매개변수:
    file_path (str): 파일이 저장될 경로
    chunk_size (int): 각 청크당 행 수
    total_rows (int): 생성할 총 행 수
    """
    
    # 카테고리 목록
    categories = ['A', 'B', 'C', 'D', 'E']
    
    # 시작 시간 기록
    start_time = time.time()
    
    # 청크 개수 계산
    num_chunks = total_rows // chunk_size
    
    for i in range(num_chunks):
        chunk_start = time.time()
        print(f"청크 생성 중: {i+1}/{num_chunks}...")

        # 각 청크의 파일 경로 설정 (일련 번호 추가)
        chunk_file_path = f"{file_path}_{i+1}.csv"
        
        with open(chunk_file_path, 'w') as f:
            # 헤더 작성
            f.write('category,value\n')

            # 한 번에 작은 그룹으로 처리 (메모리 효율성)
            for j in range(0, chunk_size, 10000):
                rows_to_write = min(10000, chunk_size - j)

                # 카테고리 생성
                cat_indices = np.random.randint(0, len(categories), rows_to_write)
                # 값 생성
                values = np.random.rand(rows_to_write) * 100

                # 데이터를 직접 문자열로 변환하여 파일에 쓰기
                for k in range(rows_to_write):
                    f.write(f"{categories[cat_indices[k]]},{values[k]:.6f}\n")

                # 작은 청크 처리 후 메모리 정리
                del cat_indices, values

        chunk_end = time.time()
        print(f"청크 {i+1} 완료: {chunk_file_path}, {chunk_end - chunk_start:.2f}초 소요")
        gc.collect()  # 명시적 가비지 컬렉션

    total_time = time.time() - start_time
    print(f"총 {num_chunks}개 파일 생성 완료 (경로: {file_path})")
    print(f"총 소요 시간: {total_time:.2f}초, 평균 초당 {total_rows/total_time:.2f}행 생성")


# 1억개 레코드 생성시에는 아래 코드 실행
generate_large_csv_in_chunks("large_file_100M")

# 10억개 파일 생성시에는 위 코드 코멘트하고 아래 코드 코멘트 삭제후 실행
# generate_large_csv_in_chunks("large_file_1B", chunk_size=10_000_000, total_rows=1_000_000_000)
