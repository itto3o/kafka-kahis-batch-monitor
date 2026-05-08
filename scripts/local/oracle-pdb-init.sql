-- ============================================================
-- SYSDBA로 실행: DPL, LSFARM PDB 생성 + 각 PDB에 m2msys 사용자/권한
-- M2MSYS PDB는 ORACLE_DATABASE 환경변수로 이미 생성됨
--
-- 실행 예:
--   docker exec -i kahis-batch-monitor-oracle sqlplus -s / as sysdba @/scripts/oracle-pdb-init.sql
-- ============================================================

WHENEVER SQLERROR CONTINUE
SET ECHO ON
SET FEEDBACK ON

-- ============================================================
-- 1. CDB 레벨 설정
-- ============================================================
-- OMF(Oracle Managed Files) 강제 — PDB 생성 시 datafile 경로 자동 결정
ALTER SYSTEM SET DB_CREATE_FILE_DEST = '/opt/oracle/oradata/FREE' SCOPE=BOTH;

-- DB Link 이름이 GLOBAL_NAME과 달라도 허용
ALTER SYSTEM SET GLOBAL_NAMES=FALSE SCOPE=BOTH;

-- ============================================================
-- 2. DPL PDB 생성 (이미 있으면 스킵 후 OPEN)
-- ============================================================
CREATE PLUGGABLE DATABASE DPL ADMIN USER pdb_admin IDENTIFIED BY pdb_admin;
ALTER PLUGGABLE DATABASE DPL OPEN;
ALTER PLUGGABLE DATABASE DPL SAVE STATE;

-- ============================================================
-- 3. LSFARM PDB 생성
-- ============================================================
CREATE PLUGGABLE DATABASE LSFARM ADMIN USER pdb_admin IDENTIFIED BY pdb_admin;
ALTER PLUGGABLE DATABASE LSFARM OPEN;
ALTER PLUGGABLE DATABASE LSFARM SAVE STATE;

-- ============================================================
-- 4. DPL PDB의 USERS tablespace + m2msys 사용자 + 권한
-- ============================================================
ALTER SESSION SET CONTAINER = DPL;
CREATE TABLESPACE USERS DATAFILE SIZE 100M AUTOEXTEND ON NEXT 50M MAXSIZE UNLIMITED;
CREATE USER m2msys IDENTIFIED BY m2msys DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO m2msys;
GRANT CREATE DATABASE LINK TO m2msys;

-- ============================================================
-- 5. LSFARM PDB의 USERS tablespace + m2msys 사용자 + 권한
-- ============================================================
ALTER SESSION SET CONTAINER = LSFARM;
CREATE TABLESPACE USERS DATAFILE SIZE 100M AUTOEXTEND ON NEXT 50M MAXSIZE UNLIMITED;
CREATE USER m2msys IDENTIFIED BY m2msys DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO m2msys;

-- ============================================================
-- 6. listener에 새 PDB 등록 강제
-- ============================================================
ALTER SESSION SET CONTAINER = CDB$ROOT;
ALTER SYSTEM REGISTER;

-- ============================================================
-- 7. 검증
-- ============================================================
SELECT NAME, OPEN_MODE FROM V$PDBS ORDER BY NAME;

EXIT