-- ============================================================
-- LSFARM PDB의 m2msys 계정으로 실행
-- 실행 예:
--   docker exec -i kahis-batch-monitor-oracle sqlplus -s m2msys/m2msys@//localhost:1521/LSFARM @/scripts/oracle-lsfarm-schema.sql
-- ============================================================

WHENEVER SQLERROR CONTINUE
SET ECHO ON

-- 멱등
DROP TABLE TN_FRMHS CASCADE CONSTRAINTS;
DROP TABLE TN_FARMHS_NO_MAPNG CASCADE CONSTRAINTS;
DROP TABLE EAI_TN_FARM_INFO CASCADE CONSTRAINTS;

WHENEVER SQLERROR EXIT SQL.SQLCODE

-- ============================================================
-- 테이블: 매퍼 selectFarmIdLsfarm/selectFarmInfo의 @DL_LSFARM DB Link로 접근
-- ============================================================
CREATE TABLE TN_FRMHS (
  FRMHS_SN         NUMBER NOT NULL,
  USE_AT           VARCHAR2(1) NOT NULL
);

CREATE TABLE TN_FARMHS_NO_MAPNG (
  FRMHS_SN         NUMBER NOT NULL,
  CNTC_FRMHS_NO    VARCHAR2(40) NOT NULL,
  USE_AT           VARCHAR2(1) NOT NULL,
  INSTT_SE_CODE    VARCHAR2(2) NOT NULL
);

-- selectFarmInfo가 호출되진 않지만 매퍼와 정합성 유지
CREATE TABLE EAI_TN_FARM_INFO (
  FARM_NO            VARCHAR2(40) NOT NULL,
  FARM_NM            VARCHAR2(100),
  BRD_OWNER_NM       VARCHAR2(50),
  FRMHS_STTUS_CODE   VARCHAR2(10),
  RE_PREARNGE_DE     DATE,
  UPDT_DE            DATE
);

-- ============================================================
-- 시드
-- ============================================================
INSERT INTO TN_FRMHS VALUES (1001, 'Y');
INSERT INTO TN_FRMHS VALUES (1002, 'Y');
INSERT INTO TN_FRMHS VALUES (1099, 'Y');

INSERT INTO TN_FARMHS_NO_MAPNG VALUES (1001, 'LSF-1001', 'Y', '03');
INSERT INTO TN_FARMHS_NO_MAPNG VALUES (1002, 'LSF-1002', 'Y', '03');
INSERT INTO TN_FARMHS_NO_MAPNG VALUES (1099, 'LSF-9999', 'Y', '03');

INSERT INTO EAI_TN_FARM_INFO VALUES ('LSF-1001', '가금류농장A', '홍길동', 'NORMAL', SYSDATE + 30, SYSDATE);
INSERT INTO EAI_TN_FARM_INFO VALUES ('LSF-1002', '한우농장B', '김철수', 'NORMAL', SYSDATE + 30, SYSDATE);
INSERT INTO EAI_TN_FARM_INFO VALUES ('LSF-9999', '신규농장C', '이영희', 'NORMAL', SYSDATE + 30, SYSDATE);

COMMIT;

PROMPT === LSFARM row counts ===
SELECT 'TN_FRMHS' AS T, COUNT(*) AS C FROM TN_FRMHS
UNION ALL SELECT 'TN_FARMHS_NO_MAPNG', COUNT(*) FROM TN_FARMHS_NO_MAPNG
UNION ALL SELECT 'EAI_TN_FARM_INFO', COUNT(*) FROM EAI_TN_FARM_INFO;

EXIT