-- ============================================================
-- DPL PDB의 m2msys 계정으로 실행 (메인 connection이 가리키는 DB)
-- 실행 예:
--   docker exec -i kahis-batch-monitor-oracle sqlplus -s m2msys/m2msys@//localhost:1521/DPL @/scripts/oracle-dpl-schema.sql
-- ============================================================

WHENEVER SQLERROR CONTINUE
SET ECHO ON

-- 멱등
DROP TABLE TN_MOBILE_BLVSTCK_HIST CASCADE CONSTRAINTS;
DROP TABLE TN_BLVSTCK CASCADE CONSTRAINTS;
DROP TABLE VIEW_FRMHS CASCADE CONSTRAINTS;
DROP DATABASE LINK M2MSYS;
DROP DATABASE LINK DL_LSFARM;

WHENEVER SQLERROR EXIT SQL.SQLCODE

-- ============================================================
-- DPL이 보유하는 테이블
-- ============================================================
-- TN_MOBILE_BLVSTCK_HIST는 매퍼에서 @M2MSYS로 호출되므로 M2MSYS PDB에 위치
-- (oracle-m2msys-schema.sql 참조)

-- 매퍼 selectFarmIdDpl이 직접 조회
CREATE TABLE TN_BLVSTCK (
  FRMHS_NO         VARCHAR2(20) NOT NULL
);

-- 매퍼 selectFarmIdDpl/selectFarmIdLsfarm의 메인 DB쪽 join 대상
CREATE TABLE VIEW_FRMHS (
  FRMHS_NO         VARCHAR2(20) NOT NULL,
  FDB_FRMHS_SN     NUMBER NOT NULL
);

-- ============================================================
-- DB Link: DPL → M2MSYS, DPL → LSFARM
-- 같은 컨테이너의 다른 PDB로 가는 link. EZ Connect 표기
-- ============================================================
CREATE DATABASE LINK M2MSYS
  CONNECT TO m2msys IDENTIFIED BY m2msys
  USING 'localhost:1521/M2MSYS';

CREATE DATABASE LINK DL_LSFARM
  CONNECT TO m2msys IDENTIFIED BY m2msys
  USING 'localhost:1521/LSFARM';

-- ============================================================
-- 시드: 농가번호 매핑 (DPL 자체 테이블)
-- ============================================================
INSERT INTO VIEW_FRMHS VALUES ('20418398', 1001);
INSERT INTO VIEW_FRMHS VALUES ('00129932', 1002);
INSERT INTO VIEW_FRMHS VALUES ('99999999', 1099);

INSERT INTO TN_BLVSTCK VALUES ('20418398');
INSERT INTO TN_BLVSTCK VALUES ('00129932');
INSERT INTO TN_BLVSTCK VALUES ('99999999');

COMMIT;

-- ============================================================
-- 검증: DB Link 동작 확인
-- ============================================================
PROMPT === DPL local row counts ===
SELECT 'TN_BLVSTCK' AS T, COUNT(*) AS C FROM TN_BLVSTCK
UNION ALL SELECT 'VIEW_FRMHS', COUNT(*) FROM VIEW_FRMHS;

PROMPT === DB Link M2MSYS HIST 조회 ===
SELECT COUNT(*) AS HIST_FROM_M2MSYS FROM TN_MOBILE_BLVSTCK_HIST@M2MSYS;

PROMPT === DB Link M2MSYS FRMHS 조회 ===
SELECT FRMHS_NO FROM TN_MOBILE_FRMHS_INFO@M2MSYS WHERE FRMHS_NO = '20418398';

PROMPT === DB Link DL_LSFARM 동작 확인 ===
SELECT TFNM.CNTC_FRMHS_NO
FROM TN_FRMHS@DL_LSFARM TF, TN_FARMHS_NO_MAPNG@DL_LSFARM TFNM
WHERE TF.FRMHS_SN = TFNM.FRMHS_SN
  AND TF.USE_AT = 'Y'
  AND TFNM.USE_AT = 'Y'
  AND TFNM.INSTT_SE_CODE = '03'
  AND TF.FRMHS_SN = 1001;

EXIT